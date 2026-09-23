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
using kvstore::ClusterStatusRequest;
using kvstore::ClusterStatusResponse;
using kvstore::DeleteRequest;
using kvstore::DeleteResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::HeartbeatRequest;
using kvstore::HeartbeatResponse;
using kvstore::KVStore;
using kvstore::ListKeysRequest;
using kvstore::ListKeysResponse;
using kvstore::LogEntry;
using kvstore::SetRequest;
using kvstore::SetResponse;
using kvstore::VoteRequest;
using kvstore::VoteResponse;

struct TlsConfig
{
    std::string ca_file;
    std::string certificate_file;
    std::string private_key_file;

    bool Enabled() const
    {
        return !ca_file.empty() && !certificate_file.empty() && !private_key_file.empty();
    }
};

static std::string ReadTextFile(const std::string &path)
{
    std::ifstream input(path);
    if (!input)
        throw std::runtime_error("failed to read TLS file: " + path);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

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
    static constexpr const char *kDeleteMarker = "__raft_kv_delete__";
    // Applied state machine - only reflects committed log entries.
    std::unordered_map<std::string, std::string> store;
    std::mutex store_mutex;

    // Outbound connections to every other node in the cluster.
    std::vector<std::shared_ptr<KVStore::Stub>> peer_stubs;
    std::mutex peers_mutex;
    std::filesystem::path peers_file;
    std::filesystem::file_time_type peers_file_timestamp{};

    std::vector<std::shared_ptr<KVStore::Stub>> PeerSnapshot()
    {
        std::lock_guard<std::mutex> lock(peers_mutex);
        return peer_stubs;
    }

    void ReloadPeersIfChanged()
    {
        if (peers_file.empty() || !std::filesystem::exists(peers_file))
            return;
        const auto timestamp = std::filesystem::last_write_time(peers_file);
        if (timestamp == peers_file_timestamp)
            return;

        std::ifstream input(peers_file);
        std::vector<std::shared_ptr<KVStore::Stub>> updated;
        std::string address;
        while (std::getline(input, address))
        {
            if (address.empty() || address[0] == '#')
                continue;
            std::shared_ptr<grpc::ChannelCredentials> credentials;
            if (tls_config.Enabled())
            {
                grpc::SslCredentialsOptions options;
                options.pem_root_certs = ReadTextFile(tls_config.ca_file);
                options.pem_cert_chain = ReadTextFile(tls_config.certificate_file);
                options.pem_private_key = ReadTextFile(tls_config.private_key_file);
                credentials = grpc::SslCredentials(options);
            }
            else
            {
                credentials = grpc::InsecureChannelCredentials();
            }
            updated.emplace_back(KVStore::NewStub(grpc::CreateChannel(address, credentials)));
        }
        if (updated.empty())
            throw std::runtime_error("membership file must contain at least one peer");
        {
            std::lock_guard<std::mutex> lock(peers_mutex);
            peer_stubs = std::move(updated);
            std::lock_guard<std::mutex> log_lock(log_mutex);
            next_index.assign(peer_stubs.size(), log_offset + (int)log_entries.size());
            match_index.assign(peer_stubs.size(), snapshot_last_index);
        }
        peers_file_timestamp = timestamp;
        std::cout << "Reloaded " << peer_stubs.size() << " peers from " << peers_file << std::endl;
    }

    // Core Raft state.
    std::atomic<NodeState> state{NodeState::FOLLOWER};
    int current_term = 0;
    int voted_for = -1;
    int node_id;
    std::mutex election_mutex;

    // Tracks the last time we heard from a leader (used for election timeouts).
    std::chrono::steady_clock::time_point last_heartbeat;
    std::mutex heartbeat_mutex;
    std::atomic<bool> running{true};

    // Replicated log and commit tracking.
    std::vector<LogEntry> log_entries;
    int log_offset = 0; // absolute index represented by log_entries[0]
    int snapshot_last_index = -1;
    int snapshot_last_term = 0;
    int commit_index = -1; // highest log index known to be committed
    int last_applied = -1; // highest log index applied to the state machine
    std::mutex log_mutex;
    std::vector<int> next_index;  // leader-only: next log index to send to each peer
    std::vector<int> match_index; // leader-only: highest log index known to be replicated on each peer
    std::filesystem::path data_directory;
    std::ofstream wal;
    TlsConfig tls_config;

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
        LoadSnapshot();
        const auto metadata_path = data_directory / "raft.meta";
        {
            std::ifstream metadata(metadata_path);
            if (metadata)
            {
                int recovered_term = 0;
                metadata >> recovered_term >> voted_for;
                current_term = recovered_term;
            }
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

    void LoadSnapshot()
    {
        const auto snapshot_path = data_directory / "raft.snapshot";
        std::ifstream input(snapshot_path, std::ios::binary);
        if (!input)
            return;

        std::uint32_t count = 0;
        input.read(reinterpret_cast<char *>(&snapshot_last_index), sizeof(snapshot_last_index));
        input.read(reinterpret_cast<char *>(&snapshot_last_term), sizeof(snapshot_last_term));
        input.read(reinterpret_cast<char *>(&count), sizeof(count));
        if (!input || count > 10000000)
            throw std::runtime_error("invalid Raft snapshot header");

        for (std::uint32_t i = 0; i < count; ++i)
        {
            std::uint32_t key_size = 0;
            std::uint32_t value_size = 0;
            input.read(reinterpret_cast<char *>(&key_size), sizeof(key_size));
            input.read(reinterpret_cast<char *>(&value_size), sizeof(value_size));
            if (!input || key_size > 64 * 1024 * 1024 || value_size > 64 * 1024 * 1024)
                throw std::runtime_error("invalid Raft snapshot record");
            std::string key(key_size, '\0');
            std::string value(value_size, '\0');
            input.read(key.data(), key_size);
            input.read(value.data(), value_size);
            if (!input)
                throw std::runtime_error("truncated Raft snapshot");
            store.emplace(std::move(key), std::move(value));
        }
        log_offset = snapshot_last_index + 1;
        commit_index = snapshot_last_index;
        last_applied = snapshot_last_index;
    }

    void CompactSnapshot()
    {
        if (commit_index - log_offset < 100)
            return;

        const int target = commit_index - 50;
        if (target < log_offset || target >= log_offset + static_cast<int>(log_entries.size()))
            return;
        for (const auto &peer_match : match_index)
        {
            if (peer_match < target)
                return;
        }

        const auto snapshot_path = data_directory / "raft.snapshot";
        const auto temporary = data_directory / "raft.snapshot.tmp";
        const auto snapshot_term = log_entries[target - log_offset].term();
        {
            std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
            const auto count = static_cast<std::uint32_t>(store.size());
            output.write(reinterpret_cast<const char *>(&target), sizeof(target));
            output.write(reinterpret_cast<const char *>(&snapshot_term), sizeof(snapshot_term));
            output.write(reinterpret_cast<const char *>(&count), sizeof(count));
            for (const auto &[key, value] : store)
            {
                const auto key_size = static_cast<std::uint32_t>(key.size());
                const auto value_size = static_cast<std::uint32_t>(value.size());
                output.write(reinterpret_cast<const char *>(&key_size), sizeof(key_size));
                output.write(reinterpret_cast<const char *>(&value_size), sizeof(value_size));
                output.write(key.data(), static_cast<std::streamsize>(key.size()));
                output.write(value.data(), static_cast<std::streamsize>(value.size()));
            }
            output.flush();
            if (!output)
                throw std::runtime_error("failed to write Raft snapshot");
        }
        std::filesystem::rename(temporary, snapshot_path);

        log_entries.erase(log_entries.begin(), log_entries.begin() + (target - log_offset + 1));
        log_offset = target + 1;
        snapshot_last_index = target;
        snapshot_last_term = snapshot_term;
        RewriteWal();
        std::cout << "Compacted Raft log through index " << target << std::endl;
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
            const auto &e = log_entries[last_applied - log_offset];
            std::lock_guard<std::mutex> store_lock(store_mutex);
            if (e.value() == kDeleteMarker)
                store.erase(e.key());
            else
                store[e.key()] = e.value();
            std::cout << "APPLIED index=" << last_applied << " " << e.key() << std::endl;
        }
    }

    // Sends AppendEntries to a single peer, backing off and retrying with an
    // earlier prevLogIndex whenever the peer reports a log mismatch. This is
    // how a peer that has fallen behind automatically catches up.
    bool SendAppendEntries(size_t peer_idx)
    {
        const auto peers = PeerSnapshot();
        if (peer_idx >= peers.size())
            return false;
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
                if (ni - 1 == snapshot_last_index)
                    req.set_prev_log_term(snapshot_last_term);
                else if (ni - 1 >= log_offset)
                    req.set_prev_log_term(log_entries[ni - 1 - log_offset].term());
                else
                    req.set_prev_log_term(0);
                for (int i = ni; i < log_offset + (int)log_entries.size(); i++)
                {
                    *req.add_entries() = log_entries[i - log_offset];
                }
                req.set_leader_commit(commit_index);
            }

            AppendEntriesResponse resp;
            ClientContext ctx;
            Status status = peers[peer_idx]->AppendEntries(&ctx, req, &resp);

            if (!status.ok())
                return false; // peer unreachable this round

            if (resp.success())
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                match_index[peer_idx] = log_offset + (int)log_entries.size() - 1;
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

        for (auto &stub : PeerSnapshot())
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

        int total_nodes = (int)PeerSnapshot().size() + 1;
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
                next_index[i] = log_offset + (int)log_entries.size();
                match_index[i] = snapshot_last_index;
            }
        }
        else
        {
            state = NodeState::FOLLOWER;
            std::cout << "Election failed (" << votes << "/" << total_nodes
                      << " votes) - reverting to FOLLOWER" << std::endl;
        }
    }

    bool CommitEntry(const LogEntry &entry)
    {
        if (state != NodeState::LEADER)
            return false;

        int new_index;
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            log_entries.push_back(entry);
            PersistRecord(entry);
            new_index = log_offset + (int)log_entries.size() - 1;
        }

        int acks = 1;
        for (size_t i = 0; i < PeerSnapshot().size(); i++)
        {
            if (SendAppendEntries(i))
                acks++;
        }
        const int majority = ((int)PeerSnapshot().size() + 1) / 2 + 1;
        if (acks < majority)
            return false;

        {
            std::lock_guard<std::mutex> lock(log_mutex);
            commit_index = std::max(commit_index, new_index);
            ApplyCommitted();
            CompactSnapshot();
        }
        for (size_t i = 0; i < PeerSnapshot().size(); i++)
            SendAppendEntries(i);
        return true;
    }

public:
    KVStoreServiceImpl(int my_port, const std::vector<std::string> &peer_addresses,
                       const std::vector<int> &peer_port_ids,
                       const std::filesystem::path &storage_path,
                       const TlsConfig &security_config,
                       const std::filesystem::path &membership_path)
    {
        node_id = my_port;
        data_directory = storage_path;
        tls_config = security_config;
        peers_file = membership_path;
        last_heartbeat = std::chrono::steady_clock::now();
        LoadPersistentState();

        for (const auto &addr : peer_addresses)
        {
            std::shared_ptr<grpc::ChannelCredentials> credentials;
            if (tls_config.Enabled())
            {
                grpc::SslCredentialsOptions options;
                options.pem_root_certs = ReadTextFile(tls_config.ca_file);
                options.pem_cert_chain = ReadTextFile(tls_config.certificate_file);
                options.pem_private_key = ReadTextFile(tls_config.private_key_file);
                credentials = grpc::SslCredentials(options);
            }
            else
            {
                credentials = grpc::InsecureChannelCredentials();
            }
            auto channel = grpc::CreateChannel(addr, credentials);
            peer_stubs.emplace_back(KVStore::NewStub(channel));
        }

        next_index.assign(peer_stubs.size(), log_offset + (int)log_entries.size());
        match_index.assign(peer_stubs.size(), snapshot_last_index);

        // Background thread: watches for election timeouts and triggers
        // an election if we haven't heard from a leader recently enough.
        std::thread([this]()
                    {
            while (running) {
                try { ReloadPeersIfChanged(); } catch (const std::exception& error) {
                    std::cerr << "Membership reload failed: " << error.what() << std::endl;
                }
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
                    for (auto& stub : PeerSnapshot()) {
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
            const bool snapshot_matches = prev_index == snapshot_last_index && prev_term == snapshot_last_term;
            const bool log_matches = prev_index >= log_offset &&
                                     prev_index < log_offset + (int)log_entries.size() &&
                                     log_entries[prev_index - log_offset].term() == prev_term;
            if (!snapshot_matches && !log_matches)
            {
                response->set_term(current_term);
                response->set_success(false);
                return Status::OK;
            }
        }

        // Drop any conflicting entries and append the new ones.
        const int prefix_size = prev_index < log_offset ? 0 : prev_index - log_offset + 1;
        log_entries.resize(prefix_size);
        for (const auto &e : request->entries())
        {
            log_entries.push_back(e);
        }
        RewriteWal();

        if (request->leader_commit() > commit_index)
        {
            const int last_log_index = log_offset + (int)log_entries.size() - 1;
            commit_index = std::min(request->leader_commit(),
                                    std::max(snapshot_last_index, last_log_index));
        }
        ApplyCommitted();

        response->set_term(current_term);
        response->set_success(true);
        response->set_match_index(std::max(snapshot_last_index,
                           log_offset + (int)log_entries.size() - 1));
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

    Status ListKeys(ServerContext *context, const ListKeysRequest *request, ListKeysResponse *response) override
    {
        std::lock_guard<std::mutex> lock(store_mutex);
        std::vector<std::string> keys;
        keys.reserve(store.size());
        for (const auto &[key, value] : store)
        {
            keys.push_back(key);
        }
        std::sort(keys.begin(), keys.end());
        for (const auto &key : keys)
        {
            response->add_keys(key);
        }
        return Status::OK;
    }

    Status GetClusterStatus(ServerContext *context, const ClusterStatusRequest *request, ClusterStatusResponse *response) override
    {
        response->set_node_id(node_id);
        response->set_current_term(current_term);
        response->set_commit_index(commit_index);
        response->set_is_leader(state == NodeState::LEADER);
        response->set_leader_id(state == NodeState::LEADER ? node_id : -1);
        return Status::OK;
    }

    Status Set(ServerContext *context, const SetRequest *request, SetResponse *response) override
    {
        if (state != NodeState::LEADER)
        {
            response->set_success(false);
            return Status::OK;
        }

        LogEntry entry;
        entry.set_term(current_term);
        entry.set_key(request->key());
        entry.set_value(request->value());
        response->set_success(CommitEntry(entry));
        return Status::OK;
    }

    Status Delete(ServerContext *context, const DeleteRequest *request, DeleteResponse *response) override
    {
        if (state != NodeState::LEADER)
        {
            response->set_success(false);
            return Status::OK;
        }

        LogEntry entry;
        entry.set_term(current_term);
        entry.set_key(request->key());
        entry.set_value(kDeleteMarker);
        response->set_success(CommitEntry(entry));
        return Status::OK;
    }
};

void RunServer(const std::string &port, const std::vector<std::string> &peer_addresses,
               const std::vector<int> &peer_ids, const TlsConfig &tls_config,
               const std::filesystem::path &membership_path)
{
    std::string server_address = "0.0.0.0:" + port;
    KVStoreServiceImpl service(std::stoi(port), peer_addresses, peer_ids,
                               std::filesystem::path("data") / ("node_" + port), tls_config,
                               membership_path);

    ServerBuilder builder;
    if (tls_config.Enabled())
    {
        grpc::SslServerCredentialsOptions options;
        options.pem_root_certs = ReadTextFile(tls_config.ca_file);
        grpc::SslServerCredentialsOptions::PemKeyCertPair key_cert;
        key_cert.private_key = ReadTextFile(tls_config.private_key_file);
        key_cert.cert_chain = ReadTextFile(tls_config.certificate_file);
        options.pem_key_cert_pairs.push_back(std::move(key_cert));
        options.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
        builder.AddListeningPort(server_address, grpc::SslServerCredentials(options));
    }
    else
    {
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    }
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
    TlsConfig tls_config;
    std::filesystem::path membership_path;

    for (int i = 2; i < argc; i++)
    {
        if (std::string(argv[i]) == "--ca" && i + 1 < argc)
        {
            tls_config.ca_file = argv[++i];
            continue;
        }
        if (std::string(argv[i]) == "--cert" && i + 1 < argc)
        {
            tls_config.certificate_file = argv[++i];
            continue;
        }
        if (std::string(argv[i]) == "--key" && i + 1 < argc)
        {
            tls_config.private_key_file = argv[++i];
            continue;
        }
        if (std::string(argv[i]) == "--peers-file" && i + 1 < argc)
        {
            membership_path = argv[++i];
            continue;
        }
        peer_addresses.push_back("localhost:" + std::string(argv[i]));
        peer_ids.push_back(std::stoi(argv[i]));
    }

    if (tls_config.Enabled())
        std::cout << "TLS/mTLS enabled" << std::endl;
    RunServer(port, peer_addresses, peer_ids, tls_config, membership_path);
    return 0;
}