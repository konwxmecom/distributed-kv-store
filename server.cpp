#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <random>
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include "runtime_config.h"

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
    static constexpr std::size_t kMaxKeyBytes = 4096;
    static constexpr std::size_t kMaxValueBytes = 1024 * 1024;
    static constexpr std::size_t kMaxRequestIdBytes = 256;
    // Applied state machine - only reflects committed log entries.
    std::unordered_map<std::string, std::string> store;
    std::mutex store_mutex;

    struct WriteRequestResult {
        bool success = false;
        int leader_id = -1;
        std::string client_id;
        std::uint64_t request_sequence = 0;
    };

    struct SnapshotState {
        std::unordered_map<std::string, std::string> store;
        std::unordered_map<std::string, WriteRequestResult> completed_requests;
        std::unordered_map<std::string, std::uint64_t> client_sequences;
    };

    std::unordered_map<std::string, WriteRequestResult> completed_write_requests;
    std::unordered_map<std::string, std::uint64_t> completed_client_sequences;
    std::mutex request_cache_mutex;
    std::mutex client_write_mutex;

    static std::string WriteIdentity(const std::string &request_id,
                                     const std::string &client_id,
                                     std::uint64_t request_sequence)
    {
        if (!client_id.empty())
            return "session/" + std::to_string(client_id.size()) + ":" + client_id + ":" +
                   std::to_string(request_sequence);
        return request_id;
    }

    bool TryReplayRequest(const std::string &identity, WriteRequestResult *result)
    {
        if (identity.empty())
            return false;
        std::lock_guard<std::mutex> lock(request_cache_mutex);
        const auto it = completed_write_requests.find(identity);
        if (it == completed_write_requests.end())
            return false;
        if (result)
            *result = it->second;
        return true;
    }

    void RecordWriteRequestResult(const LogEntry &entry, bool success, int leader_id)
    {
        const std::string identity = WriteIdentity(entry.request_id(), entry.client_id(), entry.request_sequence());
        if (identity.empty())
            return;
        std::lock_guard<std::mutex> lock(request_cache_mutex);
        completed_write_requests[identity] =
            WriteRequestResult{success, leader_id, entry.client_id(), entry.request_sequence()};
        if (!entry.client_id().empty())
            completed_client_sequences[entry.client_id()] =
                std::max(completed_client_sequences[entry.client_id()], entry.request_sequence());
    }

    bool IsStaleClientSequence(const std::string &client_id, std::uint64_t sequence)
    {
        if (client_id.empty())
            return false;
        std::lock_guard<std::mutex> lock(request_cache_mutex);
        const auto it = completed_client_sequences.find(client_id);
        return it != completed_client_sequences.end() && sequence <= it->second;
    }

    // Outbound connections to every other node in the cluster.
    std::vector<std::shared_ptr<KVStore::Stub>> peer_stubs;
    std::vector<std::string> peer_addresses;
    std::vector<int> peer_node_ids;
    std::mutex peers_mutex;
    std::filesystem::path peers_file;
    std::filesystem::file_time_type peers_file_timestamp{};

    std::vector<std::shared_ptr<KVStore::Stub>> PeerSnapshot()
    {
        std::lock_guard<std::mutex> lock(peers_mutex);
        return peer_stubs;
    }

    bool IsConfiguredPeer(int candidate_id)
    {
        std::lock_guard<std::mutex> lock(peers_mutex);
        return candidate_id != node_id &&
               std::find(peer_node_ids.begin(), peer_node_ids.end(), candidate_id) != peer_node_ids.end();
    }

    std::shared_ptr<KVStore::Stub> FindLeaderStub()
    {
        std::lock_guard<std::mutex> lock(peers_mutex);
        for (size_t i = 0; i < peer_stubs.size(); ++i)
        {
            ClusterStatusRequest status_req;
            ClusterStatusResponse status_resp;
            ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
            const auto status = peer_stubs[i]->GetClusterStatus(&ctx, status_req, &status_resp);
            if (status.ok() && status_resp.is_leader())
                return peer_stubs[i];
        }
        return nullptr;
    }

    template <typename Request, typename Response>
    bool ForwardToLeader(const Request &request, Response *response,
                         std::function<grpc::Status(grpc::ClientContext *, const Request &, Response *)> call)
    {
        if (state == NodeState::LEADER)
            return false;

        auto leader_stub = FindLeaderStub();
        if (!leader_stub)
            return false;

        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        const auto status = call(&ctx, request, response);
        if (!status.ok())
            return false;
        return true;
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
        std::vector<int> updated_ids;
        std::string address;
        while (std::getline(input, address))
        {
            if (address.empty() || address[0] == '#')
                continue;
            const auto separator = address.rfind(':');
            if (separator == std::string::npos || separator + 1 == address.size())
                throw std::runtime_error("invalid peer endpoint in membership file: " + address);
            updated_ids.push_back(std::stoi(address.substr(separator + 1)));
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
            peer_node_ids = std::move(updated_ids);
            std::lock_guard<std::mutex> log_lock(log_mutex);
            next_index.assign(peer_stubs.size(), log_offset + (int)log_entries.size());
            match_index.assign(peer_stubs.size(), snapshot_last_index);
        }
        peers_file_timestamp = timestamp;
        std::cout << "Reloaded " << peer_stubs.size() << " peers from " << peers_file << std::endl;
    }

    // Core Raft state.
    std::atomic<NodeState> state{NodeState::FOLLOWER};
    std::atomic<bool> leader_ready{false};
    int current_term = 0;
    int voted_for = -1;
    int known_leader_id = -1;
    int node_id;
    std::mutex election_mutex;

    // Tracks the last time we heard from a leader (used for election timeouts).
    std::chrono::steady_clock::time_point last_heartbeat;
    std::mutex heartbeat_mutex;
    std::atomic<bool> running{true};
    std::condition_variable lifecycle_cv;
    std::mutex lifecycle_mutex;
    std::thread election_thread;
    std::thread quorum_thread;

    // Replicated log and commit tracking.
    std::vector<LogEntry> log_entries;
    int log_offset = 0; // absolute index represented by log_entries[0]
    int snapshot_last_index = -1;
    int snapshot_last_term = 0;
    bool snapshot_has_versioned_format = false;
    int commit_index = -1; // highest log index known to be committed
    int last_applied = -1; // highest log index applied to the state machine
    std::mutex log_mutex;
    std::mutex replication_mutex;
    std::vector<int> next_index;  // leader-only: next log index to send to each peer
    std::vector<int> match_index; // leader-only: highest log index known to be replicated on each peer
    std::filesystem::path data_directory;
    std::ofstream wal;
    TlsConfig tls_config;

    static std::uint32_t ComputeCrc32(const std::string &payload)
    {
        constexpr std::uint32_t polynomial = 0xEDB88320u;
        std::uint32_t crc = 0xFFFFFFFFu;
        for (const unsigned char byte : payload)
        {
            crc ^= byte;
            for (int bit = 0; bit < 8; ++bit)
            {
                if (crc & 1u)
                    crc = (crc >> 1) ^ polynomial;
                else
                    crc >>= 1;
            }
        }
        return crc ^ 0xFFFFFFFFu;
    }

    static void SyncFilePath(const std::filesystem::path &path)
    {
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0)
            throw std::system_error(errno, std::generic_category(), "failed to open for fsync: " + path.string());

        const int sync_result = ::fsync(fd);
        const int sync_error = errno;
        const int close_result = ::close(fd);
        if (sync_result != 0)
            throw std::system_error(sync_error, std::generic_category(), "failed to fsync: " + path.string());
        if (close_result != 0)
            throw std::system_error(errno, std::generic_category(), "failed to close: " + path.string());
    }

    static void SyncDirectoryPath(const std::filesystem::path &path)
    {
        const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
        if (fd < 0)
            throw std::system_error(errno, std::generic_category(), "failed to open directory for fsync: " + path.string());
        const int sync_result = ::fsync(fd);
        const int sync_error = errno;
        const int close_result = ::close(fd);
        if (sync_result != 0)
            throw std::system_error(sync_error, std::generic_category(), "failed to fsync directory: " + path.string());
        if (close_result != 0)
            throw std::system_error(errno, std::generic_category(), "failed to close directory: " + path.string());
    }

    static bool ReadRecord(std::ifstream &input, std::string &record)
    {
        std::uint32_t size = 0;
        std::uint32_t checksum = 0;
        input.read(reinterpret_cast<char *>(&size), sizeof(size));
        if (!input)
            return false;
        if (size > 64 * 1024 * 1024)
            throw std::runtime_error("WAL record is unreasonably large");
        input.read(reinterpret_cast<char *>(&checksum), sizeof(checksum));
        if (!input)
            throw std::runtime_error("WAL contains a truncated checksum");
        record.resize(size);
        input.read(record.data(), static_cast<std::streamsize>(size));
        if (!input)
            throw std::runtime_error("WAL contains a truncated record");
        if (ComputeCrc32(record) != checksum)
            return false;
        return true;
    }

    static SnapshotState ParseSnapshotPayload(const std::string &payload)
    {
        SnapshotState snapshot;
        if (payload.size() > 256 * 1024 * 1024)
            throw std::runtime_error("snapshot payload exceeds the supported size limit");
        constexpr char magic[] = "RKVSNAP1";
        if (payload.size() >= sizeof(magic) - 1 &&
            payload.compare(0, sizeof(magic) - 1, magic, sizeof(magic) - 1) == 0)
        {
            kvstore::SnapshotData data;
            if (!data.ParseFromArray(payload.data() + sizeof(magic) - 1,
                                     static_cast<int>(payload.size() - (sizeof(magic) - 1))))
                throw std::runtime_error("snapshot protobuf payload is invalid");
            for (const auto &entry : data.entries())
                snapshot.store[entry.key()] = entry.value();
            for (const auto &result : data.completed_requests())
            {
                const std::string identity = WriteIdentity(
                    result.request_id(), result.client_id(), result.request_sequence());
                snapshot.completed_requests[identity] = WriteRequestResult{
                    result.success(), result.leader_id(), result.client_id(), result.request_sequence()};
            }
            for (const auto &[client_id, sequence] : data.client_sequences())
                snapshot.client_sequences[client_id] = sequence;
            return snapshot;
        }
        if (payload.empty())
            return snapshot;

        std::size_t offset = 0;
        std::uint32_t count = 0;
        if (payload.size() < sizeof(count))
            throw std::runtime_error("snapshot payload is truncated");
        std::memcpy(&count, payload.data() + offset, sizeof(count));
        offset += sizeof(count);

        for (std::uint32_t i = 0; i < count; ++i)
        {
            if (offset + sizeof(std::uint32_t) * 2 > payload.size())
                throw std::runtime_error("snapshot payload has truncated key/value sizes");
            std::uint32_t key_size = 0;
            std::uint32_t value_size = 0;
            std::memcpy(&key_size, payload.data() + offset, sizeof(key_size));
            offset += sizeof(key_size);
            std::memcpy(&value_size, payload.data() + offset, sizeof(value_size));
            offset += sizeof(value_size);

            if (key_size > 64 * 1024 * 1024 || value_size > 64 * 1024 * 1024)
                throw std::runtime_error("snapshot payload contains an invalid record size");
            if (offset + key_size + value_size > payload.size())
                throw std::runtime_error("snapshot payload is truncated");

            std::string key(payload.data() + offset, key_size);
            offset += key_size;
            std::string value(payload.data() + offset, value_size);
            offset += value_size;
            snapshot.store.emplace(std::move(key), std::move(value));
        }
        return snapshot;
    }

    static std::string SerializeSnapshotPayload(
        const std::unordered_map<std::string, std::string> &snapshot_store,
        const std::unordered_map<std::string, WriteRequestResult> &completed_requests,
        const std::unordered_map<std::string, std::uint64_t> &client_sequences)
    {
        kvstore::SnapshotData data;
        for (const auto &[key, value] : snapshot_store)
        {
            auto *entry = data.add_entries();
            entry->set_key(key);
            entry->set_value(value);
        }
        for (const auto &[identity, result] : completed_requests)
        {
            auto *saved_result = data.add_completed_requests();
            if (result.client_id.empty())
                saved_result->set_request_id(identity);
            else
                saved_result->set_client_id(result.client_id);
            saved_result->set_request_sequence(result.request_sequence);
            saved_result->set_success(result.success);
            saved_result->set_leader_id(result.leader_id);
        }
        for (const auto &[client_id, sequence] : client_sequences)
            (*data.mutable_client_sequences())[client_id] = sequence;
        std::string serialized;
        if (!data.SerializeToString(&serialized))
            throw std::runtime_error("failed to serialize snapshot data");
        return std::string("RKVSNAP1") + serialized;
    }

    std::string SerializeSnapshotPayload()
    {
        std::unordered_map<std::string, std::string> snapshot_store;
        std::unordered_map<std::string, WriteRequestResult> completed_requests;
        std::unordered_map<std::string, std::uint64_t> client_sequences;
        {
            std::lock_guard<std::mutex> lock(store_mutex);
            snapshot_store = store;
        }
        {
            std::lock_guard<std::mutex> lock(request_cache_mutex);
            completed_requests = completed_write_requests;
            client_sequences = completed_client_sequences;
        }
        return SerializeSnapshotPayload(snapshot_store, completed_requests, client_sequences);
    }

    static void WriteRecord(std::ofstream &output, const std::filesystem::path &path, const std::string &payload)
    {
        const auto size = static_cast<std::uint32_t>(payload.size());
        const auto checksum = ComputeCrc32(payload);
        output.clear();
        output.write(reinterpret_cast<const char *>(&size), sizeof(size));
        output.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
        output.write(payload.data(), static_cast<std::streamsize>(payload.size()));
        output.flush();
        if (!output)
            throw std::runtime_error("failed to flush WAL");
        SyncFilePath(path);
    }

    void PersistRecord(const LogEntry &entry)
    {
        LogEntry indexed_entry = entry;
        if (indexed_entry.log_index_plus_one() == 0)
        {
            const int index = log_offset + static_cast<int>(log_entries.size()) - 1;
            indexed_entry.set_log_index_plus_one(static_cast<std::uint64_t>(index) + 1);
        }
        std::string serialized;
        if (!indexed_entry.SerializeToString(&serialized))
            throw std::runtime_error("failed to serialize WAL record");
        wal.clear();
        WriteRecord(wal, data_directory / "raft.wal", serialized);
    }

    void RewriteWal()
    {
        const auto wal_path = data_directory / "raft.wal";
        const auto temporary_path = data_directory / "raft.wal.tmp";
        wal.close();
        wal.clear();
        try
        {
            {
                std::ofstream output(temporary_path, std::ios::binary | std::ios::trunc);
                if (!output)
                    throw std::runtime_error("failed to create temporary WAL: " + temporary_path.string());
                for (std::size_t i = 0; i < log_entries.size(); ++i)
                {
                    LogEntry indexed_entry = log_entries[i];
                    indexed_entry.set_log_index_plus_one(static_cast<std::uint64_t>(log_offset + static_cast<int>(i)) + 1);
                    std::string serialized;
                    if (!indexed_entry.SerializeToString(&serialized))
                        throw std::runtime_error("failed to serialize WAL record");
                    const auto size = static_cast<std::uint32_t>(serialized.size());
                    const auto checksum = ComputeCrc32(serialized);
                    output.write(reinterpret_cast<const char *>(&size), sizeof(size));
                    output.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
                    output.write(serialized.data(), static_cast<std::streamsize>(serialized.size()));
                }
                output.flush();
                if (!output)
                    throw std::runtime_error("failed to rewrite WAL");
                SyncFilePath(temporary_path);
            }
            std::filesystem::rename(temporary_path, wal_path);
            SyncDirectoryPath(data_directory);
            wal = std::ofstream(wal_path, std::ios::binary | std::ios::app);
            if (!wal)
                throw std::runtime_error("failed to reopen WAL");
        }
        catch (...)
        {
            if (!wal.is_open())
            {
                wal.clear();
                wal.open(wal_path, std::ios::binary | std::ios::app);
            }
            throw;
        }
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
                if (!(metadata >> recovered_term >> voted_for) || recovered_term < 0 ||
                    (metadata >> std::ws && !metadata.eof()))
                    throw std::runtime_error("invalid Raft metadata file: " + metadata_path.string());
                current_term = recovered_term;
            }
        }

        const auto wal_path = data_directory / "raft.wal";
        std::ifstream input(wal_path, std::ios::binary);
        std::string record;
        bool truncated_tail = false;
        int wal_index = snapshot_has_versioned_format ? 0 : snapshot_last_index + 1;
        while (input)
        {
            try
            {
                if (!ReadRecord(input, record))
                {
                    truncated_tail = true;
                    break;
                }
            }
            catch (const std::runtime_error &error)
            {
                truncated_tail = true;
                std::cerr << "Ignoring trailing corrupt WAL entry: " << error.what() << std::endl;
                break;
            }

            LogEntry entry;
            if (!entry.ParseFromString(record))
            {
                truncated_tail = true;
                std::cerr << "Ignoring trailing invalid WAL entry" << std::endl;
                break;
            }
            if (entry.log_index_plus_one() != 0)
            {
                const auto absolute_index = entry.log_index_plus_one() - 1;
                if (absolute_index > static_cast<std::uint64_t>(std::numeric_limits<int>::max()))
                    throw std::runtime_error("WAL log index exceeds supported range");
                if (absolute_index >= static_cast<std::uint64_t>(snapshot_last_index + 1))
                {
                    if (absolute_index != static_cast<std::uint64_t>(log_offset + static_cast<int>(log_entries.size())))
                        throw std::runtime_error("WAL log indices are not contiguous");
                    log_entries.push_back(std::move(entry));
                }
                wal_index = static_cast<int>(absolute_index + 1);
            }
            else if (wal_index > snapshot_last_index)
            {
                entry.set_log_index_plus_one(static_cast<std::uint64_t>(wal_index) + 1);
                log_entries.push_back(std::move(entry));
                ++wal_index;
            }
        }
        if (truncated_tail)
        {
            input.close();
            std::cerr << "Truncating corrupt WAL tail and rewriting valid log prefix" << std::endl;
            RewriteWal();
        }
        else
        {
            wal.open(wal_path, std::ios::binary | std::ios::app);
            if (!wal)
                throw std::runtime_error("failed to open WAL: " + wal_path.string());
        }
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
            SyncFilePath(temporary);
        }
        std::filesystem::rename(temporary, metadata_path);
        SyncDirectoryPath(data_directory);
    }

    void LoadSnapshot()
    {
        const auto snapshot_path = data_directory / "raft.snapshot";
        std::ifstream input(snapshot_path, std::ios::binary);
        if (!input)
            return;

        const std::string file_data{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        constexpr char magic[] = "RKVSNP01";
        if (file_data.size() >= sizeof(magic) - 1 &&
            file_data.compare(0, sizeof(magic) - 1, magic, sizeof(magic) - 1) == 0)
        {
            snapshot_has_versioned_format = true;
            constexpr std::size_t header_size = sizeof(magic) - 1 + sizeof(snapshot_last_index) +
                                                sizeof(snapshot_last_term) + sizeof(std::uint32_t) * 2;
            if (file_data.size() < header_size)
                throw std::runtime_error("truncated Raft snapshot header");
            std::size_t offset = sizeof(magic) - 1;
            std::uint32_t payload_size = 0;
            std::uint32_t checksum = 0;
            std::memcpy(&snapshot_last_index, file_data.data() + offset, sizeof(snapshot_last_index));
            offset += sizeof(snapshot_last_index);
            std::memcpy(&snapshot_last_term, file_data.data() + offset, sizeof(snapshot_last_term));
            offset += sizeof(snapshot_last_term);
            std::memcpy(&payload_size, file_data.data() + offset, sizeof(payload_size));
            offset += sizeof(payload_size);
            std::memcpy(&checksum, file_data.data() + offset, sizeof(checksum));
            offset += sizeof(checksum);
            if (payload_size > 256 * 1024 * 1024 ||
                file_data.size() != header_size + payload_size)
                throw std::runtime_error("invalid Raft snapshot payload size");
            const std::string payload = file_data.substr(offset, payload_size);
            if (ComputeCrc32(payload) != checksum)
                throw std::runtime_error("Raft snapshot checksum mismatch");
            auto snapshot = ParseSnapshotPayload(payload);
            store = std::move(snapshot.store);
            completed_write_requests = std::move(snapshot.completed_requests);
            completed_client_sequences = std::move(snapshot.client_sequences);
            log_offset = snapshot_last_index + 1;
            commit_index = snapshot_last_index;
            last_applied = snapshot_last_index;
            return;
        }

        std::istringstream legacy(file_data, std::ios::binary);
        std::uint32_t count = 0;
        legacy.read(reinterpret_cast<char *>(&snapshot_last_index), sizeof(snapshot_last_index));
        legacy.read(reinterpret_cast<char *>(&snapshot_last_term), sizeof(snapshot_last_term));
        legacy.read(reinterpret_cast<char *>(&count), sizeof(count));
        if (!legacy || count > 10000000)
            throw std::runtime_error("invalid Raft snapshot header");

        for (std::uint32_t i = 0; i < count; ++i)
        {
            std::uint32_t key_size = 0;
            std::uint32_t value_size = 0;
            legacy.read(reinterpret_cast<char *>(&key_size), sizeof(key_size));
            legacy.read(reinterpret_cast<char *>(&value_size), sizeof(value_size));
            if (!legacy || key_size > 64 * 1024 * 1024 || value_size > 64 * 1024 * 1024)
                throw std::runtime_error("invalid Raft snapshot record");
            std::string key(key_size, '\0');
            std::string value(value_size, '\0');
            legacy.read(key.data(), key_size);
            legacy.read(value.data(), value_size);
            if (!legacy)
                throw std::runtime_error("truncated Raft snapshot");
            store.emplace(std::move(key), std::move(value));
        }
        if (legacy.peek() != std::char_traits<char>::eof())
            throw std::runtime_error("unexpected trailing bytes in legacy Raft snapshot");
        log_offset = snapshot_last_index + 1;
        commit_index = snapshot_last_index;
        last_applied = snapshot_last_index;
    }

    void PersistSnapshotState(const std::unordered_map<std::string, std::string> &snapshot_store,
                             const std::unordered_map<std::string, WriteRequestResult> &completed_requests,
                             const std::unordered_map<std::string, std::uint64_t> &client_sequences,
                             int last_index, int last_term)
    {
        const auto snapshot_path = data_directory / "raft.snapshot";
        const auto temporary = data_directory / "raft.snapshot.tmp";
        {
            std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
            const std::string payload = SerializeSnapshotPayload(snapshot_store, completed_requests,
                                                                 client_sequences);
            if (payload.size() > std::numeric_limits<std::uint32_t>::max())
                throw std::runtime_error("Raft snapshot exceeds the supported size limit");
            const auto payload_size = static_cast<std::uint32_t>(payload.size());
            const auto checksum = ComputeCrc32(payload);
            output.write("RKVSNP01", 8);
            output.write(reinterpret_cast<const char *>(&last_index), sizeof(last_index));
            output.write(reinterpret_cast<const char *>(&last_term), sizeof(last_term));
            output.write(reinterpret_cast<const char *>(&payload_size), sizeof(payload_size));
            output.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
            output.write(payload.data(), static_cast<std::streamsize>(payload.size()));
            output.flush();
            if (!output)
                throw std::runtime_error("failed to write Raft snapshot");
            SyncFilePath(temporary);
        }
        std::filesystem::rename(temporary, snapshot_path);
        SyncDirectoryPath(data_directory);
    }

    void CompactSnapshot()
    {
        if (commit_index - log_offset < 100)
            return;

        const int target = commit_index;
        if (target < log_offset || target >= log_offset + static_cast<int>(log_entries.size()))
            return;
        for (const auto &peer_match : match_index)
        {
            if (peer_match < target)
                return;
        }

        const auto snapshot_term = log_entries[target - log_offset].term();
        std::unordered_map<std::string, WriteRequestResult> completed_requests;
        std::unordered_map<std::string, std::uint64_t> client_sequences;
        {
            std::lock_guard<std::mutex> request_lock(request_cache_mutex);
            completed_requests = completed_write_requests;
            client_sequences = completed_client_sequences;
        }
        try
        {
            PersistSnapshotState(store, completed_requests, client_sequences, target, snapshot_term);
        }
        catch (const std::exception &error)
        {
            std::cerr << "Snapshot persistence failed; retaining WAL: " << error.what() << std::endl;
            return;
        }

        log_entries.erase(log_entries.begin(), log_entries.begin() + (target - log_offset + 1));
        log_offset = target + 1;
        snapshot_last_index = target;
        snapshot_last_term = snapshot_term;
        try
        {
            RewriteWal();
        }
        catch (const std::exception &error)
        {
            std::cerr << "WAL compaction failed; durable snapshot remains usable: "
                      << error.what() << std::endl;
        }
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
            if (e.no_op())
                continue;
            if (!e.request_id().empty() || !e.client_id().empty())
                RecordWriteRequestResult(e, true, known_leader_id);
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
    bool SendInstallSnapshot(size_t peer_idx)
    {
        const auto peers = PeerSnapshot();
        if (peer_idx >= peers.size())
            return false;

        kvstore::InstallSnapshotRequest req;
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            req.set_leader_term(current_term);
            req.set_leader_id(node_id);
            req.set_last_included_index(snapshot_last_index);
            req.set_last_included_term(snapshot_last_term);
            req.set_snapshot_data(SerializeSnapshotPayload());
            req.set_done(true);
        }

        kvstore::InstallSnapshotResponse resp;
        ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        const auto status = peers[peer_idx]->InstallSnapshot(&ctx, req, &resp);
        if (!status.ok())
            return false;
        if (!resp.success())
            return false;

        std::lock_guard<std::mutex> lock(log_mutex);
        next_index[peer_idx] = std::max(next_index[peer_idx], snapshot_last_index + 1);
        match_index[peer_idx] = snapshot_last_index;
        return true;
    }

    bool SendAppendEntries(size_t peer_idx)
    {
        const auto peers = PeerSnapshot();
        if (peer_idx >= peers.size())
            return false;
        while (true)
        {
            int ni;
            bool install_snapshot = false;
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                ni = next_index[peer_idx];
                install_snapshot = ni <= snapshot_last_index;
            }
            if (install_snapshot)
                return SendInstallSnapshot(peer_idx);

            AppendEntriesRequest req;
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
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
            Status status = peers[peer_idx]->AppendEntries(&ctx, req, &resp);

            if (!status.ok())
                return false; // peer unreachable this round

            if (resp.term() > req.leader_term())
            {
                std::lock_guard<std::mutex> election_lock(election_mutex);
                if (resp.term() > current_term)
                {
                    current_term = resp.term();
                    voted_for = -1;
                    PersistMetadata();
                    state = NodeState::FOLLOWER;
                    leader_ready = false;
                }
                return false;
            }

            if (resp.success())
            {
                std::lock_guard<std::mutex> lock(log_mutex);
                match_index[peer_idx] = resp.match_index();
                next_index[peer_idx] = resp.match_index() + 1;
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
        int observed_term;
        int candidate_last_log_index;
        int candidate_last_log_term;
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (state == NodeState::LEADER)
                return;
            observed_term = current_term;
        }
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            candidate_last_log_index = log_offset + static_cast<int>(log_entries.size()) - 1;
            candidate_last_log_term = log_entries.empty() ? snapshot_last_term : log_entries.back().term();
        }

        const auto peers = PeerSnapshot();
        const int total_nodes = static_cast<int>(peers.size()) + 1;
        const int majority = (total_nodes / 2) + 1;
        const int proposed_term = observed_term + 1;
        int pre_votes = 1;
        for (const auto &stub : peers)
        {
            VoteRequest request;
            request.set_candidate_term(proposed_term);
            request.set_candidate_id(node_id);
            request.set_candidate_last_log_index(candidate_last_log_index);
            request.set_candidate_last_log_term(candidate_last_log_term);
            request.set_pre_vote(true);
            VoteResponse response;
            ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
            const auto status = stub->RequestVote(&context, request, &response);
            if (status.ok() && response.voter_term() > observed_term)
            {
                std::lock_guard<std::mutex> lock(election_mutex);
                if (response.voter_term() > current_term)
                {
                    current_term = response.voter_term();
                    voted_for = -1;
                    PersistMetadata();
                    state = NodeState::FOLLOWER;
                }
                return;
            }
            if (status.ok() && response.vote_granted())
                ++pre_votes;
        }

        if (pre_votes < majority)
        {
            std::cout << "Pre-vote failed (" << pre_votes << "/" << total_nodes
                      << ") - remaining FOLLOWER" << std::endl;
            return;
        }

        int my_term;
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (state == NodeState::LEADER || current_term != observed_term)
                return;
            current_term = proposed_term;
            voted_for = node_id;
            PersistMetadata();
            state = NodeState::CANDIDATE;
            my_term = current_term;
            std::cout << "Becoming CANDIDATE for term " << current_term << std::endl;
        }

        int votes = 1; // vote for self
        for (const auto &stub : peers)
        {
            VoteRequest req;
            req.set_candidate_term(my_term);
            req.set_candidate_id(node_id);
            req.set_candidate_last_log_index(candidate_last_log_index);
            req.set_candidate_last_log_term(candidate_last_log_term);
            VoteResponse resp;
            ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
            Status status = stub->RequestVote(&ctx, req, &resp);
            if (status.ok() && resp.voter_term() > my_term)
            {
                std::lock_guard<std::mutex> lock(election_mutex);
                if (resp.voter_term() > current_term)
                {
                    current_term = resp.voter_term();
                    voted_for = -1;
                    PersistMetadata();
                }
                state = NodeState::FOLLOWER;
                return;
            }
            if (status.ok() && resp.vote_granted())
                votes++;
        }

        bool won_election = false;
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (state != NodeState::CANDIDATE || current_term != my_term)
                return;

            if (votes >= majority)
            {
                state = NodeState::LEADER;
                leader_ready = false;
                known_leader_id = node_id;
                std::cout << "*** BECAME LEADER for term " << current_term
                          << " with " << votes << "/" << total_nodes << " votes ***" << std::endl;
                won_election = true;
            }
            else
            {
                state = NodeState::FOLLOWER;
                leader_ready = false;
                std::cout << "Election failed (" << votes << "/" << total_nodes
                          << " votes) - reverting to FOLLOWER" << std::endl;
            }
        }

        if (won_election)
        {
            {
                std::lock_guard<std::mutex> log_lock(log_mutex);
                for (size_t i = 0; i < next_index.size(); i++)
                {
                    next_index[i] = log_offset + static_cast<int>(log_entries.size());
                    match_index[i] = snapshot_last_index;
                }
            }

            LogEntry no_op;
            no_op.set_term(my_term);
            no_op.set_no_op(true);
            if (CommitEntry(no_op))
            {
                std::lock_guard<std::mutex> lock(election_mutex);
                if (state == NodeState::LEADER && current_term == my_term)
                    leader_ready = true;
            }
        }
    }

    bool CommitEntry(const LogEntry &entry)
    {
        std::lock_guard<std::mutex> replication_lock(replication_mutex);
        if (state != NodeState::LEADER)
            return false;

        int new_index = -1;
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            const std::string identity = WriteIdentity(
                entry.request_id(), entry.client_id(), entry.request_sequence());
            if (!identity.empty())
            {
                for (std::size_t i = 0; i < log_entries.size(); ++i)
                {
                    if (WriteIdentity(log_entries[i].request_id(), log_entries[i].client_id(),
                                      log_entries[i].request_sequence()) == identity)
                    {
                        new_index = log_offset + static_cast<int>(i);
                        break;
                    }
                }
            }
            if (new_index < 0)
            {
                log_entries.push_back(entry);
                try
                {
                    PersistRecord(entry);
                }
                catch (const std::exception &error)
                {
                    log_entries.pop_back();
                    std::cerr << "Failed to persist Raft log entry: " << error.what() << std::endl;
                    return false;
                }
                new_index = log_offset + static_cast<int>(log_entries.size()) - 1;
            }
        }

        const auto peers = PeerSnapshot();
        for (size_t i = 0; i < peers.size(); i++)
        {
            SendAppendEntries(i);
        }

        bool committed = false;
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            const int majority = (static_cast<int>(peers.size()) + 1) / 2 + 1;
            const int last_index = log_offset + static_cast<int>(log_entries.size()) - 1;
            for (int candidate = last_index; candidate > commit_index; --candidate)
            {
                const int offset = candidate - log_offset;
                if (offset < 0 || log_entries[offset].term() != current_term)
                    continue;

                int replicas = 1;
                for (const int peer_match : match_index)
                {
                    if (peer_match >= candidate)
                        ++replicas;
                }
                if (replicas >= majority)
                {
                    commit_index = candidate;
                    ApplyCommitted();
                    CompactSnapshot();
                    break;
                }
            }
            committed = new_index <= commit_index;
        }
        for (size_t i = 0; i < peers.size(); i++)
            SendAppendEntries(i);
        if (!committed)
            std::cerr << "Write on leader " << node_id
                      << " was not committed by a current-term quorum" << std::endl;
        return committed;
    }

    bool LeaderHasQuorum()
    {
        const int total_nodes = (int)PeerSnapshot().size() + 1;
        const int majority = total_nodes / 2 + 1;
        int active_nodes = 1;
        int observed_term;
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            observed_term = current_term;
        }
        for (const auto &stub : PeerSnapshot())
        {
            HeartbeatRequest request;
            request.set_leader_port(node_id);
            request.set_leader_term(observed_term);
            HeartbeatResponse response;
            ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
            const auto status = stub->Heartbeat(&context, request, &response);
            if (status.ok() && response.alive() && response.term() == observed_term)
                active_nodes++;
            else if (status.ok() && response.term() > observed_term)
            {
                std::lock_guard<std::mutex> lock(election_mutex);
                if (response.term() > current_term)
                {
                    current_term = response.term();
                    voted_for = -1;
                    PersistMetadata();
                    state = NodeState::FOLLOWER;
                    leader_ready = false;
                }
                return false;
            }
        }
        return active_nodes >= majority;
    }

    bool ConfirmLinearizableRead()
    {
        int read_term;
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (state != NodeState::LEADER || !leader_ready)
                return false;
            read_term = current_term;
        }
        if (!LeaderHasQuorum())
            return false;
        std::lock_guard<std::mutex> lock(election_mutex);
        return state == NodeState::LEADER && leader_ready && current_term == read_term;
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
        peer_node_ids = peer_port_ids;

        next_index.assign(peer_stubs.size(), log_offset + (int)log_entries.size());
        match_index.assign(peer_stubs.size(), snapshot_last_index);

        // Background thread: watches for election timeouts and triggers
        // an election if we haven't heard from a leader recently enough.
        election_thread = std::thread([this]()
                    {
            while (running) {
                try { ReloadPeersIfChanged(); } catch (const std::exception& error) {
                    std::cerr << "Membership reload failed: " << error.what() << std::endl;
                }
                int timeout_ms = RandomElectionTimeout();
                {
                    std::unique_lock<std::mutex> wait_lock(lifecycle_mutex);
                    lifecycle_cv.wait_for(wait_lock, std::chrono::milliseconds(timeout_ms),
                                          [this] { return !running.load(); });
                }
                if (!running)
                    break;
                if (state == NodeState::LEADER) continue;
                std::chrono::steady_clock::time_point last_seen;
                {
                    std::lock_guard<std::mutex> lock(heartbeat_mutex);
                    last_seen = last_heartbeat;
                }
                auto elapsed = std::chrono::steady_clock::now() - last_seen;
                if (elapsed > std::chrono::milliseconds(timeout_ms)) StartElection();
            } })
            ;

        // Background thread: while we're leader, periodically send
        // heartbeats to all peers so they don't start their own elections.
        quorum_thread = std::thread([this]()
                    {
            while (running) {
                if (state == NodeState::LEADER) {
                    int reachable = 1;
                    int observed_term;
                    {
                        std::lock_guard<std::mutex> lock(election_mutex);
                        observed_term = current_term;
                    }
                    for (auto& stub : PeerSnapshot()) {
                        HeartbeatRequest req;
                        req.set_leader_port(node_id);
                        req.set_leader_term(observed_term);
                        HeartbeatResponse resp;
                        ClientContext ctx;
                        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
                        const auto status = stub->Heartbeat(&ctx, req, &resp);
                        if (status.ok() && resp.alive() && resp.term() == observed_term) {
                            reachable++;
                        }
                        if (status.ok() && resp.term() > observed_term) {
                            std::lock_guard<std::mutex> lock(election_mutex);
                            if (resp.term() > current_term) {
                                current_term = resp.term();
                                voted_for = -1;
                                PersistMetadata();
                            }
                            state = NodeState::FOLLOWER;
                            leader_ready = false;
                            break;
                        }
                    }
                    const int total_nodes = (int)PeerSnapshot().size() + 1;
                    const int majority = total_nodes / 2 + 1;
                    if (reachable < majority) {
                        std::lock_guard<std::mutex> lock(election_mutex);
                        if (state == NodeState::LEADER) {
                            state = NodeState::FOLLOWER;
                            leader_ready = false;
                            std::lock_guard<std::mutex> hb_lock(heartbeat_mutex);
                            last_heartbeat = std::chrono::steady_clock::now();
                            std::cout << "Leader lost quorum at term " << current_term
                                      << " - stepping down" << std::endl;
                        }
                    }
                }
                std::unique_lock<std::mutex> wait_lock(lifecycle_mutex);
                lifecycle_cv.wait_for(wait_lock, std::chrono::milliseconds(1000),
                                      [this] { return !running.load(); });
            } })
            ;
    }

    ~KVStoreServiceImpl() override
    {
        running = false;
        lifecycle_cv.notify_all();
        if (election_thread.joinable())
            election_thread.join();
        if (quorum_thread.joinable())
            quorum_thread.join();
    }

    // Follower-side handler: receives log entries (or heartbeats) from the leader.
    Status AppendEntries(ServerContext *context, const AppendEntriesRequest *request, AppendEntriesResponse *response) override
    {
        if (request->leader_id() != node_id && !IsConfiguredPeer(request->leader_id()))
            return Status(grpc::StatusCode::PERMISSION_DENIED, "AppendEntries sender is not a configured peer");
        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (request->leader_term() < current_term)
            {
                // Stale leader - reject.
                response->set_term(current_term);
                response->set_success(false);
                return Status::OK;
            }
            if (request->entries_size() > 1024)
                return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "AppendEntries batch exceeds 1024 entries");
            for (const auto &entry : request->entries())
            {
                if (entry.key().size() > kMaxKeyBytes || entry.value().size() > kMaxValueBytes ||
                    entry.request_id().size() > kMaxRequestIdBytes)
                    return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "replicated entry exceeds configured size limits");
            }
            if (request->prev_log_index() < -1 || request->leader_commit() < -1)
                return Status(grpc::StatusCode::INVALID_ARGUMENT, "AppendEntries contains a negative Raft index");
            if (request->leader_term() > current_term)
            {
                current_term = request->leader_term();
                voted_for = -1;
                try
                {
                    PersistMetadata();
                }
                catch (const std::exception &error)
                {
                    return Status(grpc::StatusCode::INTERNAL, error.what());
                }
            }
            state = NodeState::FOLLOWER;
            leader_ready = false;
            known_leader_id = request->leader_id();
        }
        {
            std::lock_guard<std::mutex> hb_lock(heartbeat_mutex);
            last_heartbeat = std::chrono::steady_clock::now();
        }

        std::lock_guard<std::mutex> log_lock(log_mutex);
        int prev_index = request->prev_log_index();
        int prev_term = request->prev_log_term();

        if (prev_index < snapshot_last_index)
        {
            response->set_term(current_term);
            response->set_success(false);
            return Status::OK;
        }

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

        // Preserve matching entries and their suffix; truncate only at the
        // first conflicting entry, as required by Raft's log-matching rule.
        const auto original_entries = log_entries;
        bool log_changed = false;
        int entry_index = prev_index + 1;
        for (const auto &entry : request->entries())
        {
            const int entry_offset = entry_index - log_offset;
            if (entry_offset >= 0 && entry_offset < static_cast<int>(log_entries.size()))
            {
                if (log_entries[entry_offset].term() == entry.term())
                {
                    ++entry_index;
                    continue;
                }
                if (entry_index <= commit_index)
                {
                    response->set_term(current_term);
                    response->set_success(false);
                    return Status::OK;
                }
                log_entries.resize(entry_offset);
            }
            LogEntry indexed_entry = entry;
            indexed_entry.set_log_index_plus_one(static_cast<std::uint64_t>(log_offset + static_cast<int>(log_entries.size())) + 1);
            log_entries.push_back(indexed_entry);
            log_changed = true;
            ++entry_index;
        }
        if (log_changed)
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
                                           prev_index + request->entries_size()));
        return Status::OK;
    }

    Status InstallSnapshot(ServerContext *context, const kvstore::InstallSnapshotRequest *request, kvstore::InstallSnapshotResponse *response) override
    {
        if (request->leader_id() != node_id && !IsConfiguredPeer(request->leader_id()))
            return Status(grpc::StatusCode::PERMISSION_DENIED, "snapshot sender is not a configured peer");

        SnapshotState snapshot;
        try
        {
            snapshot = ParseSnapshotPayload(request->snapshot_data());
        }
        catch (const std::exception &error)
        {
            return Status(grpc::StatusCode::INVALID_ARGUMENT, error.what());
        }

        {
            std::lock_guard<std::mutex> lock(election_mutex);
            if (request->leader_term() < current_term)
            {
                response->set_term(current_term);
                response->set_success(false);
                response->set_match_index(snapshot_last_index);
                return Status::OK;
            }
            if (request->leader_term() > current_term)
            {
                current_term = request->leader_term();
                voted_for = -1;
                try
                {
                    PersistMetadata();
                }
                catch (const std::exception &error)
                {
                    return Status(grpc::StatusCode::INTERNAL, error.what());
                }
            }
            state = NodeState::FOLLOWER;
            leader_ready = false;
            known_leader_id = request->leader_id();
        }

        {
            std::lock_guard<std::mutex> lock(log_mutex);
            if (request->last_included_index() <= snapshot_last_index ||
                request->last_included_index() < commit_index)
            {
                response->set_term(current_term);
                response->set_success(request->last_included_index() <= snapshot_last_index);
                response->set_match_index(snapshot_last_index);
                return Status::OK;
            }

            const int included_index = request->last_included_index();
            const int included_offset = included_index - log_offset;
            const bool suffix_matches = included_offset >= 0 &&
                included_offset < static_cast<int>(log_entries.size()) &&
                log_entries[included_offset].term() == request->last_included_term();
            try
            {
                PersistSnapshotState(snapshot.store, snapshot.completed_requests, snapshot.client_sequences,
                                     included_index, request->last_included_term());
            }
            catch (const std::exception &error)
            {
                return Status(grpc::StatusCode::INTERNAL, error.what());
            }
            if (suffix_matches)
                log_entries.erase(log_entries.begin(), log_entries.begin() + included_offset + 1);
            else
                log_entries.clear();
            log_offset = included_index + 1;
            snapshot_last_index = included_index;
            snapshot_last_term = request->last_included_term();
            commit_index = std::max(commit_index, snapshot_last_index);
            last_applied = std::max(last_applied, snapshot_last_index);
        }

        {
            std::lock_guard<std::mutex> store_lock(store_mutex);
            store = std::move(snapshot.store);
        }
        {
            std::lock_guard<std::mutex> request_lock(request_cache_mutex);
            completed_write_requests = std::move(snapshot.completed_requests);
            completed_client_sequences = std::move(snapshot.client_sequences);
        }
        try
        {
            RewriteWal();
        }
        catch (const std::exception &error)
        {
            return Status(grpc::StatusCode::INTERNAL, error.what());
        }

        response->set_term(current_term);
        response->set_success(true);
        response->set_match_index(snapshot_last_index);
        return Status::OK;
    }

    // Handles a vote request from a candidate.
    Status RequestVote(ServerContext *context, const VoteRequest *request, VoteResponse *response) override
    {
        if (request->candidate_id() != node_id && !IsConfiguredPeer(request->candidate_id()))
        {
            response->set_vote_granted(false);
            response->set_voter_term(current_term);
            return Status::OK;
        }
        std::lock_guard<std::mutex> lock(election_mutex);
        int last_log_index;
        int last_log_term;
        {
            std::lock_guard<std::mutex> log_lock(log_mutex);
            last_log_index = log_offset + static_cast<int>(log_entries.size()) - 1;
            last_log_term = log_entries.empty() ? snapshot_last_term : log_entries.back().term();
        }
        const bool candidate_log_is_up_to_date =
            request->candidate_last_log_term() > last_log_term ||
            (request->candidate_last_log_term() == last_log_term &&
             request->candidate_last_log_index() >= last_log_index);

        if (request->pre_vote())
        {
            std::chrono::steady_clock::time_point last_seen;
            {
                std::lock_guard<std::mutex> heartbeat_lock(heartbeat_mutex);
                last_seen = last_heartbeat;
            }
            const bool leader_contact_is_stale =
                std::chrono::steady_clock::now() - last_seen >= std::chrono::milliseconds(1500);
            const bool grant_pre_vote =
                request->candidate_term() > current_term &&
                candidate_log_is_up_to_date &&
                state != NodeState::LEADER &&
                leader_contact_is_stale;
            response->set_vote_granted(grant_pre_vote);
            response->set_voter_term(current_term);
            return Status::OK;
        }

        if (request->candidate_term() > current_term)
        {
            current_term = request->candidate_term();
            voted_for = -1;
            PersistMetadata();
            state = NodeState::FOLLOWER;
            leader_ready = false;
        }

        bool grant = false;
        if (request->candidate_term() >= current_term &&
            candidate_log_is_up_to_date &&
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
        if (request->leader_port() != node_id && !IsConfiguredPeer(request->leader_port()))
            return Status(grpc::StatusCode::PERMISSION_DENIED, "heartbeat sender is not a configured peer");

        std::lock_guard<std::mutex> lock(election_mutex);
        if (request->leader_term() < current_term)
        {
            response->set_alive(true);
            response->set_term(current_term);
            return Status::OK;
        }
        if (request->leader_term() > current_term)
        {
            current_term = request->leader_term();
            voted_for = -1;
            try
            {
                PersistMetadata();
            }
            catch (const std::exception &error)
            {
                return Status(grpc::StatusCode::INTERNAL, error.what());
            }
        }
        state = NodeState::FOLLOWER;
        leader_ready = false;
        known_leader_id = request->leader_port();
        std::lock_guard<std::mutex> hb_lock(heartbeat_mutex);
        last_heartbeat = std::chrono::steady_clock::now();
        response->set_alive(true);
        response->set_term(current_term);
        return Status::OK;
    }

    Status Get(ServerContext *context, const GetRequest *request, GetResponse *response) override
    {
        if (state == NodeState::LEADER && request->consistency() == kvstore::ReadConsistency::STRONG &&
            !ConfirmLinearizableRead())
            return Status(grpc::StatusCode::UNAVAILABLE, "leader cannot confirm a quorum for a linearizable read");

        if (state == NodeState::LEADER && !leader_ready)
            return Status(grpc::StatusCode::UNAVAILABLE, "leader is not ready to serve requests");

        if (state != NodeState::LEADER && !peer_stubs.empty())
        {
            if (request->consistency() == kvstore::ReadConsistency::EVENTUAL)
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
                response->set_leader_id(known_leader_id);
                return Status::OK;
            }

            auto leader_stub = FindLeaderStub();
            if (leader_stub)
            {
                GetRequest forwarded = *request;
                GetResponse forwarded_response;
                ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                const auto status = leader_stub->Get(&ctx, forwarded, &forwarded_response);
                if (status.ok())
                {
                    *response = forwarded_response;
                    return Status::OK;
                }
            }
            return Status(grpc::StatusCode::UNAVAILABLE, "leader is unavailable for a linearizable read");
        }

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
        response->set_leader_id(node_id);
        return Status::OK;
    }

    Status ListKeys(ServerContext *context, const ListKeysRequest *request, ListKeysResponse *response) override
    {
        if (state == NodeState::LEADER && request->consistency() == kvstore::ReadConsistency::STRONG &&
            !ConfirmLinearizableRead())
            return Status(grpc::StatusCode::UNAVAILABLE, "leader cannot confirm a quorum for a linearizable read");

        if (state == NodeState::LEADER && !leader_ready)
            return Status(grpc::StatusCode::UNAVAILABLE, "leader is not ready to serve requests");

        if (state != NodeState::LEADER && !peer_stubs.empty())
        {
            if (request->consistency() == kvstore::ReadConsistency::EVENTUAL)
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
                response->set_leader_id(known_leader_id);
                return Status::OK;
            }

            auto leader_stub = FindLeaderStub();
            if (leader_stub)
            {
                ListKeysRequest forwarded = *request;
                ListKeysResponse forwarded_response;
                ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                const auto status = leader_stub->ListKeys(&ctx, forwarded, &forwarded_response);
                if (status.ok())
                {
                    *response = forwarded_response;
                    return Status::OK;
                }
            }
        }

        if (state != NodeState::LEADER && !peer_stubs.empty() &&
            request->consistency() == kvstore::ReadConsistency::STRONG)
            return Status(grpc::StatusCode::UNAVAILABLE, "leader is unavailable for a linearizable read");

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
        response->set_leader_id(node_id);
        return Status::OK;
    }

    Status GetClusterStatus(ServerContext *context, const ClusterStatusRequest *request, ClusterStatusResponse *response) override
    {
        const int leader_id = state == NodeState::LEADER ? node_id : known_leader_id;
        response->set_node_id(node_id);
        response->set_current_term(current_term);
        response->set_commit_index(commit_index);
        response->set_is_leader(state == NodeState::LEADER && leader_ready);
        response->set_leader_id(leader_id);
        return Status::OK;
    }

    Status Set(ServerContext *context, const SetRequest *request, SetResponse *response) override
    {
        if (request->key().empty() || request->key().size() > kMaxKeyBytes ||
            request->value().size() > kMaxValueBytes ||
            request->request_id().size() > kMaxRequestIdBytes ||
            request->client_id().size() > kMaxRequestIdBytes ||
            (request->client_id().empty() != (request->request_sequence() == 0)))
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "key/value/request ID exceeds configured limits");

        std::lock_guard<std::mutex> write_lock(client_write_mutex);
        const std::string identity = WriteIdentity(request->request_id(), request->client_id(),
                                                   request->request_sequence());
        WriteRequestResult replay_result;
        if (TryReplayRequest(identity, &replay_result))
        {
            response->set_success(replay_result.success);
            response->set_leader_id(replay_result.leader_id);
            response->set_request_id(request->request_id());
            response->set_client_id(request->client_id());
            response->set_request_sequence(request->request_sequence());
            return Status::OK;
        }
        if (IsStaleClientSequence(request->client_id(), request->request_sequence()))
            return Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "client request sequence is older than the committed session");

        if (state == NodeState::LEADER && !leader_ready)
        {
            response->set_success(false);
            response->set_leader_id(node_id);
            response->set_request_id(request->request_id());
            response->set_client_id(request->client_id());
            response->set_request_sequence(request->request_sequence());
            return Status::OK;
        }

        if (state != NodeState::LEADER && !peer_stubs.empty())
        {
            auto leader_stub = FindLeaderStub();
            if (leader_stub)
            {
                SetRequest forwarded = *request;
                SetResponse forwarded_response;
                ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                const auto status = leader_stub->Set(&ctx, forwarded, &forwarded_response);
                if (status.ok())
                {
                    if (!forwarded_response.success())
                        std::cerr << "Leader rejected forwarded write from node " << node_id << std::endl;
                    response->set_success(forwarded_response.success());
                    response->set_leader_id(forwarded_response.leader_id());
                    response->set_request_id(forwarded_response.request_id());
                    response->set_client_id(forwarded_response.client_id());
                    response->set_request_sequence(forwarded_response.request_sequence());
                    return Status::OK;
                }
                std::cerr << "Forwarded write from node " << node_id << " failed: "
                          << status.error_message() << std::endl;
            }
            else
                std::cerr << "No leader found while forwarding write from node " << node_id << std::endl;
            response->set_success(false);
            response->set_leader_id(known_leader_id);
            response->set_request_id(request->request_id());
            response->set_client_id(request->client_id());
            response->set_request_sequence(request->request_sequence());
            return Status::OK;
        }

        LogEntry entry;
        entry.set_term(current_term);
        entry.set_key(request->key());
        entry.set_value(request->value());
        entry.set_request_id(request->request_id());
        entry.set_client_id(request->client_id());
        entry.set_request_sequence(request->request_sequence());
        const bool success = CommitEntry(entry);
        response->set_success(success);
        response->set_leader_id(node_id);
        response->set_request_id(request->request_id());
        response->set_client_id(request->client_id());
        response->set_request_sequence(request->request_sequence());
        return Status::OK;
    }

    Status Delete(ServerContext *context, const DeleteRequest *request, DeleteResponse *response) override
    {
        if (request->key().empty() || request->key().size() > kMaxKeyBytes ||
            request->request_id().size() > kMaxRequestIdBytes ||
            request->client_id().size() > kMaxRequestIdBytes ||
            (request->client_id().empty() != (request->request_sequence() == 0)))
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "key/request ID exceeds configured limits");

        std::lock_guard<std::mutex> write_lock(client_write_mutex);
        const std::string identity = WriteIdentity(request->request_id(), request->client_id(),
                                                   request->request_sequence());
        WriteRequestResult replay_result;
        if (TryReplayRequest(identity, &replay_result))
        {
            response->set_success(replay_result.success);
            response->set_leader_id(replay_result.leader_id);
            response->set_request_id(request->request_id());
            response->set_client_id(request->client_id());
            response->set_request_sequence(request->request_sequence());
            return Status::OK;
        }
        if (IsStaleClientSequence(request->client_id(), request->request_sequence()))
            return Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "client request sequence is older than the committed session");

        if (state == NodeState::LEADER && !leader_ready)
        {
            response->set_success(false);
            response->set_leader_id(node_id);
            response->set_request_id(request->request_id());
            response->set_client_id(request->client_id());
            response->set_request_sequence(request->request_sequence());
            return Status::OK;
        }

        if (state != NodeState::LEADER && !peer_stubs.empty())
        {
            auto leader_stub = FindLeaderStub();
            if (leader_stub)
            {
                DeleteRequest forwarded = *request;
                DeleteResponse forwarded_response;
                ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                const auto status = leader_stub->Delete(&ctx, forwarded, &forwarded_response);
                if (status.ok())
                {
                    response->set_success(forwarded_response.success());
                    response->set_leader_id(forwarded_response.leader_id());
                    response->set_request_id(forwarded_response.request_id());
                    response->set_client_id(forwarded_response.client_id());
                    response->set_request_sequence(forwarded_response.request_sequence());
                    return Status::OK;
                }
            }
            response->set_success(false);
            response->set_leader_id(known_leader_id);
            response->set_request_id(request->request_id());
            response->set_client_id(request->client_id());
            response->set_request_sequence(request->request_sequence());
            return Status::OK;
        }

        LogEntry entry;
        entry.set_term(current_term);
        entry.set_key(request->key());
        entry.set_value(kDeleteMarker);
        entry.set_request_id(request->request_id());
        entry.set_client_id(request->client_id());
        entry.set_request_sequence(request->request_sequence());
        const bool success = CommitEntry(entry);
        response->set_success(success);
        response->set_leader_id(node_id);
        response->set_request_id(request->request_id());
        response->set_client_id(request->client_id());
        response->set_request_sequence(request->request_sequence());
        return Status::OK;
    }
};

void RunServer(const std::string &port, const std::vector<std::string> &peer_addresses,
               const std::vector<int> &peer_ids, const TlsConfig &tls_config,
               const std::filesystem::path &membership_path,
               const std::filesystem::path &data_root)
{
    std::string server_address = "0.0.0.0:" + port;
    KVStoreServiceImpl service(std::stoi(port), peer_addresses, peer_ids,
                               data_root / ("node_" + port), tls_config,
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
    // Usage: server.exe <port> <peer1_port> <peer2_port> ... [options]
    // Or: server.exe --config node.conf
    if (argc < 2)
    {
        std::cout << "Usage: server.exe <port> <peer1_port> <peer2_port> ... [options]" << std::endl;
        return 1;
    }

    RuntimeConfig runtime_config;
    std::filesystem::path config_path;
    for (int i = 1; i + 1 < argc; ++i)
    {
        if (std::string(argv[i]) == "--config")
        {
            config_path = argv[i + 1];
            break;
        }
    }
    if (!config_path.empty())
        runtime_config = LoadRuntimeConfig(config_path);

    std::string port = runtime_config.port;
    std::vector<std::string> peer_addresses;
    std::vector<int> peer_ids;
    TlsConfig tls_config;
    std::filesystem::path membership_path = runtime_config.peers_file;
    std::filesystem::path data_root = runtime_config.data_root;
    tls_config.ca_file = runtime_config.ca_file;
    tls_config.certificate_file = runtime_config.certificate_file;
    tls_config.private_key_file = runtime_config.private_key_file;

    auto add_peer = [&](const std::string &peer) {
        const auto separator = peer.rfind(':');
        const auto address = separator == std::string::npos ? "localhost:" + peer : peer;
        peer_addresses.push_back(address);
        peer_ids.push_back(separator == std::string::npos ? std::stoi(peer) : std::stoi(peer.substr(separator + 1)));
    };
    for (const auto &peer : runtime_config.peers)
        add_peer(peer);

    int argument_index = 1;
    if (std::string(argv[1]) != "--config")
    {
        port = argv[1];
        argument_index = 2;
    }
    if (port.empty())
    {
        std::cerr << "A port is required either as an argument or in --config." << std::endl;
        return 1;
    }

    for (int i = argument_index; i < argc; i++)
    {
        if (std::string(argv[i]) == "--config" && i + 1 < argc)
        {
            ++i;
            continue;
        }
        if (std::string(argv[i]) == "--data-root" && i + 1 < argc)
        {
            data_root = argv[++i];
            continue;
        }
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
        add_peer(argv[i]);
    }

    if (tls_config.Enabled())
        std::cout << "TLS/mTLS enabled" << std::endl;
    RunServer(port, peer_addresses, peer_ids, tls_config, membership_path, data_root);
    return 0;
}