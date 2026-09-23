#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::AppendEntriesRequest;
using kvstore::AppendEntriesResponse;
using kvstore::DeleteRequest;
using kvstore::DeleteResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::HeartbeatRequest;
using kvstore::HeartbeatResponse;
using kvstore::KVStore;
using kvstore::LogEntry;
using kvstore::SetRequest;
using kvstore::SetResponse;
using kvstore::VoteRequest;
using kvstore::VoteResponse;

// The three states a Raft node can be in at any point in time.
enum class NodeState
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

// Implements the KVStore gRPC service, including the underlying Raft,
// consensus protocol (leader election + log replication).
class KVStoreServiceImpl final : public KVStore::Service
{
private:
    // Applied state machine - only reflects committed log entries.
    std::unordered_map<std::string, std::string> store;
    std::mutex store_mutex;

    // Outbound connections to every other node in the cluster.
    std::vector<std::unique_ptr<KVStore::Stub>> peer_stubs;

    // Core Raft state.
    std::atomic<NodeState> state{NodeState::FOLLOWER};
    std::atomic<int> current_term{0};
    int voted_for = -1;
    int node_id;
    std::mutex election_mutex;

    // Tracks the last time we heard from a leader (used for election timeouts).
    std::chrono::steady_clock::time_point last_heartbeat;
    std::mutex heartbeat_mutex;
    std::atomic<bool> running{true};

    // Replicated log and commit tracking.
    std::vector<LogEntry> log_entries;
    int commit_index = -1; // highest log index known to be committed
    int last_applied = -1; // highest log index applied to the state machine
    std::mutex log_mutex;
    std::vector<int> next_index;  // leader-only: next log index to send to each peer
    std::vector<int> match_index; // leader-only: highest log index known to be replicated on each peer
    std::filesystem::path data_directory;
    std::ofstream wal;

    static bool ReadRecord(std::ifstream &input, std::string &record)
    {
        std::uint32_t size = 0;
        input.read(reinterpret_cast<char *>(&size), sizeof(size));
        if (!input)
            return false;
        if (size > 64 * 1024 * 1024)
            throw std::runtime_error("WAL record is unreasonably large");
        record.resize(size);
        input.read(record.data(), size);
        if (!input)
            throw std::runtime_error("WAL contains a truncated record");
        return true;
    }

    void PersistRecord(const LogEntry &entry)
    {
        std::string serialized;
        if (!entry.SerializeToString(&serialized))
            throw std::runtime_error("failed to serialize WAL record");
        const auto size = static_cast<std::uint32_t>(serialized.size());
        wal.write(reinterpret_cast<const char *>(&size), sizeof(size));
        wal.write(serialized.data(), static_cast<std::streamsize>(serialized.size()));
        wal.flush();
        if (!wal)
            throw std::runtime_error("failed to flush WAL");
    }

    void RewriteWal()
    {
        const auto wal_path = data_directory / "raft.wal";
        const auto temporary = data_directory / "raft.wal.tmp";
        {
            std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
            for (const auto &entry : log_entries)
            {
                std::string serialized;
                if (!entry.SerializeToString(&serialized))
                    throw std::runtime_error("failed to serialize WAL record");
                const auto size = static_cast<std::uint32_t>(serialized.size());
                output.write(reinterpret_cast<const char *>(&size), sizeof(size));
                output.write(serialized.data(), static_cast<std::streamsize>(serialized.size()));
            }
            output.flush();
            if (!output)
                throw std::runtime_error("failed to rewrite WAL");
        }
        wal.close();
        std::filesystem::rename(temporary, wal_path);
        wal.open(wal_path, std::ios::binary | std::ios::app);
        if (!wal)
            throw std::runtime_error("failed to reopen WAL");
    }

    void LoadPersistentState()
    {
        std::filesystem::create_directories(data_directory);
        const auto metadata_path = data_directory / "raft.meta";
        {
            std::ifstream metadata(metadata_path);
            if (metadata)
                metadata >> current_term >> voted_for;
        }

        const auto wal_path = data_directory / "raft.wal";
        std::ifstream input(wal_path, std::ios::binary);
        std::string record;
        while (input && ReadRecord(input, record))
        {
            LogEntry entry;
            if (!entry.ParseFromString(record))
                throw std::runtime_error("WAL contains an invalid log entry");
            log_entries.push_back(std::move(entry));
        }
        wal.open(wal_path, std::ios::binary | std::ios::app);
        if (!wal)
            throw std::runtime_error("failed to open WAL: " + wal_path.string());
        std::cout << "Recovered " << log_entries.size() << " log entries from "
                  << wal_path << std::endl;
    }

    void PersistMetadata()
    {
        const auto temporary = data_directory / "raft.meta.tmp";
        const auto metadata_path = data_directory / "raft.meta";
        {
            std::ofstream metadata(temporary, std::ios::trunc);
            metadata << current_term << " " << voted_for << "\n";
            metadata.flush();
            if (!metadata)
                throw std::runtime_error("failed to write Raft metadata");
        }
        std::filesystem::rename(temporary, metadata_path);
    }

    // Generates a randomized election timeout to reduce the chance of
    // multiple nodes becoming candidates simultaneously (split votes).
    int RandomElectionTimeout()
    {
        static std::mt19937 rng(std::random_device{}());
        static std::uniform_int_distribution<int> dist(3000, 5000);
        return dist(rng);
    }

    // Applies any newly committed log entries to the state machine.
    // Caller must hold log_mutex.
    void ApplyCommitted()
    {
        while (last_applied < commit_index)
        {
            last_applied++;
            const auto &e = log_entries[last_applied];
            std::lock_guard<std::mutex> store_lock(store_mutex);
            store[e.key()] = e.value();
            std::cout << "APPLIED index=" << last_applied << " " << e.key() << "=" << e.value() << std::endl;
        }
    }

    // Sends AppendEntries to a single peer, backing off and retrying with an
    // earlier prevLogIndex whenever the peer reports a log mismatch. This is
    // how a peer that has fallen behind automatically catches up.
    bool SendAppendEntries(size_t peer_idx)
    {
        while (true)
        {
            AppendEntriesRequest req;
            int ni;
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                ni = next_index[peer_idx];
                req.set_leader_term(current_term);
                req.set_leader_id(node_id);
                req.set_prev_log_index(ni - 1);
                req.set_prev_log_term(ni - 1 >= 0 ? log_entries[ni - 1].term() : 0);
                for (int i = ni; i < (int)log_entries.size(); i++)
                {
                    *req.add_entries() = log_entries[i];
                }
                req.set_leader_commit(commit_index);
            }

            AppendEntriesResponse resp;
            ClientContext ctx;
            Status status = peer_stubs[peer_idx]->AppendEntries(&ctx, req, &resp);

            if (!status.ok())
                return false; // peer unreachable this round

            if (resp.success())
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                match_index[peer_idx] = (int)log_entries.size() - 1;
                next_index[peer_idx] = match_index[peer_idx] + 1;
                return true;
            }
            else
            {
                // Log mismatch - step back one index and retry.
                std::lock_guard<std::mutex> lock(log_mutex);
                if (next_index[peer_idx] > 0)
                {
                    next_index[peer_idx]--;
                }
                else
                {
                    return false;
                }
            }
        }
    }

    // Runs a full election cycle: increment term, vote for self, request
    // votes from every peer, and become leader on majority.
    void StartElection()
    {
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            current_term++;
            voted_for = node_id;
            PersistMetadata();
            state = NodeState::CANDIDATE;
            std::cout << "Becoming CANDIDATE for term " << current_term << std::endl;
        }

        int votes = 1; // vote for self
        int my_term = current_term;

        for (auto &stub : peer_stubs)
        {
            VoteRequest req;
            req.set_candidate_term(my_term);
            req.set_candidate_id(node_id);
            VoteResponse resp;
            ClientContext ctx;
            Status status = stub->RequestVote(&ctx, req, &resp);
            if (status.ok() && resp.vote_granted())
                votes++;
        }

        int total_nodes = (int)peer_stubs.size() + 1;
        int majority = (total_nodes / 2) + 1;

        std::lock_guard<std::mutex> lock(election_mutex);
        // Bail out if another node already became leader while we were voting.
        if (state != NodeState::CANDIDATE || current_term != my_term)
            return;

        if (votes >= majority)
        {
            state = NodeState::LEADER;
            std::cout << "*** BECAME LEADER for term " << current_term
                      << " with " << votes << "/" << total_nodes << " votes ***" << std::endl;

            // Reset per-peer replication state now that we're leader.
            std::lock_guard<std::mutex> log_lock(log_mutex);
            for (size_t i = 0; i < next_index.size(); i++)
            {
                next_index[i] = (int)log_entries.size();
                match_index[i] = -1;
            }
        }
        else
        {
            state = NodeState::FOLLOWER;
            std::cout << "Election failed (" << votes << "/" << total_nodes
                      << " votes) - reverting to FOLLOWER" << std::endl;
        }
    }

public:
    KVStoreServiceImpl(int my_port, const std::vector<std::string> &peer_addresses,
                       const std::vector<int> &peer_port_ids,
                       const std::filesystem::path &storage_path)
    {
        node_id = my_port;
        data_directory = storage_path;
        last_heartbeat = std::chrono::steady_clock::now();
        LoadPersistentState();

        for (const auto &addr : peer_addresses)
        {
            auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            peer_stubs.push_back(KVStore::NewStub(channel));
        }

        next_index.assign(peer_stubs.size(), 0);
        match_index.assign(peer_stubs.size(), -1);

        // Background thread: watches for election timeouts and triggers
        // an election if we haven't heard from a leader recently enough.
        std::thread([this]()
                    {
            while (running) {
                int timeout_ms = RandomElectionTimeout();
                std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
                if (state == NodeState::LEADER) continue;
                std::lock_guard<std::mutex> lock(heartbeat_mutex);
                auto elapsed = std::chrono::steady_clock::now() - last_heartbeat;
                if (elapsed > std::chrono::milliseconds(timeout_ms)) StartElection();
            } })
            .detach();

        // Background thread: while we're leader, periodically send
        // heartbeats to all peers so they don't start their own elections.
        std::thread([this]()
                    {
            while (running) {
                if (state == NodeState::LEADER) {
                    for (auto& stub : peer_stubs) {
                        HeartbeatRequest req;
                        req.set_leader_port(node_id);
                        req.set_leader_term(current_term);
                        HeartbeatResponse resp;
                        ClientContext ctx;
                        stub->Heartbeat(&ctx, req, &resp);
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            } })
            .detach();
    }

    // Follower-side handler: receives log entries (or heartbeats) from the leader.
    Status AppendEntries(ServerContext *context, const AppendEntriesRequest *request, AppendEntriesResponse *response) override
    {
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (request->leader_term() < current_term)
            {
                // Stale leader - reject.
                response->set_term(current_term);
                response->set_success(false);
                return Status::OK;
            }
            current_term = request->leader_term();
            state = NodeState::FOLLOWER;
        }
        {
            std::lock_guard<std::mutex> hb_lock(heartbeat_mutex);
            last_heartbeat = std::chrono::steady_clock::now();
        }

        std::lock_guard<std::mutex> log_lock(log_mutex);
        int prev_index = request->prev_log_index();
        int prev_term = request->prev_log_term();

        // Consistency check: reject if our log doesn't match what the leader expects.
        // The leader will back off and retry with an earlier index.
        if (prev_index >= 0)
        {
            if ((int)log_entries.size() <= prev_index || log_entries[prev_index].term() != prev_term)
            {
                response->set_term(current_term);
                response->set_success(false);
                return Status::OK;
            }
        }

        // Drop any conflicting entries and append the new ones.
        log_entries.resize(prev_index + 1);
        for (const auto &e : request->entries())
        {
            log_entries.push_back(e);
        }
        RewriteWal();

        if (request->leader_commit() > commit_index)
        {
            commit_index = std::min(request->leader_commit(), (int)log_entries.size() - 1);
        }
        ApplyCommitted();

        response->set_term(current_term);
        response->set_success(true);
        response->set_match_index((int)log_entries.size() - 1);
        return Status::OK;
    }

    // Handles a vote request from a candidate.
    Status RequestVote(ServerContext *context, const VoteRequest *request, VoteResponse *response) override
    {
        std::lock_guard<std::mutex> lock(election_mutex);
        if (request->candidate_term() > current_term)
        {
            current_term = request->candidate_term();
            voted_for = -1;
            PersistMetadata();
            state = NodeState::FOLLOWER;
        }
        bool grant = false;
        if (request->candidate_term() >= current_term &&
            (voted_for == -1 || voted_for == request->candidate_id()))
        {
            grant = true;
            voted_for = request->candidate_id();
            PersistMetadata();
            std::lock_guard<std::mutex> hb_lock(heartbeat_mutex);
            last_heartbeat = std::chrono::steady_clock::now();
        }
        response->set_vote_granted(grant);
        response->set_voter_term(current_term);
        std::cout << "Vote request from node " << request->candidate_id()
                  << " (term " << request->candidate_term() << ") -> "
                  << (grant ? "GRANTED" : "DENIED") << std::endl;
        return Status::OK;
    }

    Status Heartbeat(ServerContext *context, const HeartbeatRequest *request, HeartbeatResponse *response) override
    {
        std::lock_guard<std::mutex> lock(election_mutex);
        if (request->leader_term() >= current_term)
        {
            current_term = request->leader_term();
            state = NodeState::FOLLOWER;
        }
        std::lock_guard<std::mutex> hb_lock(heartbeat_mutex);
        last_heartbeat = std::chrono::steady_clock::now();
        response->set_alive(true);
        return Status::OK;
    }

    Status Get(ServerContext *context, const GetRequest *request, GetResponse *response) override
    {
        std::lock_guard<std::mutex> lock(store_mutex);
        auto it = store.find(request->key());
        if (it != store.end())
        {
            response->set_value(it->second);
            response->set_found(true);
        }
        else
        {
            response->set_found(false);
        }
        return Status::OK;
    }

    Status Set(ServerContext *context, const SetRequest *request, SetResponse *response) override
    {
        LogEntry entry;
        entry.set_term(current_term);
        entry.set_key(request->key());
        entry.set_value(request->value());

        int new_index;
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            log_entries.push_back(entry);
            PersistRecord(entry);
            new_index = (int)log_entries.size() - 1;
        }
        std::cout << "LOG APPEND index=" << new_index << " " << request->key() << "=" << request->value() << std::endl;

        if (state == NodeState::LEADER)
        {
            int acks = 1; // count ourselves
            for (size_t i = 0; i < peer_stubs.size(); i++)
            {
                if (SendAppendEntries(i))
                    acks++;
            }
            int majority = ((int)peer_stubs.size() + 1) / 2 + 1;
            if (acks >= majority)
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                if (new_index > commit_index)
                    commit_index = new_index;
                ApplyCommitted();
            }

            // Send a second round so followers receive the updated commit
            // index even if their log already matches (no new entries to send).
            for (size_t i = 0; i < peer_stubs.size(); i++)
            {
                SendAppendEntries(i);
            }
        }

        response->set_success(true);
        return Status::OK;
    }

    Status Delete(ServerContext *context, const DeleteRequest *request, DeleteResponse *response) override
    {
        std::lock_guard<std::mutex> lock(store_mutex);
        size_t erased = store.erase(request->key());
        response->set_success(erased > 0);
        return Status::OK;
    }
};

void RunServer(const std::string &port, const std::vector<std::string> &peer_addresses,
               const std::vector<int> &peer_ids)
{
    std::string server_address = "0.0.0.0:" + port;
    KVStoreServiceImpl service(std::stoi(port), peer_addresses, peer_ids,
                               std::filesystem::path("data") / ("node_" + port));

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    // If another process is already bound to this port, BuildAndStart()
    // returns nullptr instead of throwing. Fail loudly instead of crashing
    // on server->Wait() below.
    if (!server)
    {
        std::cerr << "Failed to start server on " << server_address
                  << " - the port may already be in use." << std::endl;
        return;
    }

    std::cout << "Server listening on " << server_address << " [starting as FOLLOWER]" << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    // Usage: server.exe <port> <peer1_port> <peer2_port> ...
    // All nodes are started symmetrically; the cluster elects its own leader.
    if (argc < 2)
    {
        std::cout << "Usage: server.exe <port> <peer1_port> <peer2_port> ..." << std::endl;
        return 1;
    }

    std::string port = argv[1];
    std::vector<std::string> peer_addresses;
    std::vector<int> peer_ids;

    for (int i = 2; i < argc; i++)
    {
        peer_addresses.push_back("localhost:" + std::string(argv[i]));
        peer_ids.push_back(std::stoi(argv[i]));
    }

    RunServer(port, peer_addresses, peer_ids);
    return 0;
}