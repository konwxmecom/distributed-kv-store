#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "../kvstore.grpc.pb.h"

namespace {

std::string ResolveServerBinary() {
    std::vector<std::filesystem::path> candidates = {
        std::filesystem::current_path() / "server",
        std::filesystem::current_path() / "build" / "server",
        std::filesystem::current_path().parent_path() / "build" / "server",
        std::filesystem::current_path() / "build" / "DistributedKVStore" / "server",
        std::filesystem::current_path() / "build" / "src" / "server"
    };

    for (const auto& candidate : candidates) {
        if (std::filesystem::exists(candidate)) {
            return candidate.string();
        }
    }

    return "server";
}

bool WaitForServer(const std::string& address, int timeout_seconds = 20) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);
    while (std::chrono::steady_clock::now() < deadline) {
        kvstore::HeartbeatRequest req;
        req.set_leader_port(0);
        req.set_leader_term(0);
        kvstore::HeartbeatResponse resp;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
        const auto status = stub.Heartbeat(&context, req, &resp);
        if (status.ok() && resp.alive()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool WaitForLeader(const std::string& address, int timeout_seconds = 10) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);
    while (std::chrono::steady_clock::now() < deadline) {
        kvstore::SetRequest req;
        req.set_key("__raft_test_readiness__");
        req.set_value("ready");
        kvstore::SetResponse resp;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
        const auto status = stub.Set(&context, req, &resp);
        if (status.ok() && resp.success()) {
            kvstore::DeleteRequest delete_req;
            delete_req.set_key("__raft_test_readiness__");
            kvstore::DeleteResponse delete_resp;
            grpc::ClientContext delete_context;
            delete_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
            stub.Delete(&delete_context, delete_req, &delete_resp);
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

pid_t StartServer(const std::string& port, const std::vector<std::string>& peers) {
    std::vector<std::string> argv;
    argv.push_back(ResolveServerBinary());
    argv.push_back(port);
    for (const auto& peer : peers) {
        argv.push_back(peer);
    }

    std::vector<char*> cargs;
    for (auto& arg : argv) {
        cargs.push_back(arg.data());
    }
    cargs.push_back(nullptr);

    pid_t pid = fork();
    if (pid == 0) {
        execv(cargs[0], cargs.data());
        std::perror("execv server");
        std::_Exit(1);
    }
    return pid;
}

void StopServer(pid_t pid) {
    if (pid > 0) {
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
    }
}

struct TestServer {
    explicit TestServer(const std::string& port, bool reset_storage = true)
        : port(port), pid(-1) {
        if (reset_storage) {
            std::error_code error;
            std::filesystem::remove_all(std::filesystem::path("data") / ("node_" + port), error);
        }
        pid = StartServer(port, {});
    }

    ~TestServer() {
        StopServer(pid);
    }

    std::string port;
    pid_t pid;
};

struct TestCluster {
    explicit TestCluster(const std::vector<std::string>& ports) {
        for (const auto& port : ports) {
            std::vector<std::string> peers;
            for (const auto& peer_port : ports) {
                if (peer_port != port) {
                    peers.push_back("localhost:" + peer_port);
                }
            }
            pids.push_back(StartServer(port, peers));
        }
    }

    ~TestCluster() {
        for (const auto pid : pids) {
            StopServer(pid);
        }
    }

    std::vector<pid_t> pids;
};

TEST(RaftIntegration, LeaderSetAndGetRoundTrip) {
    const std::string port = "18051";
    const std::string address = "localhost:" + port;
    TestServer server(port);
    ASSERT_GT(server.pid, 0);
    ASSERT_TRUE(WaitForServer(address));
    ASSERT_TRUE(WaitForLeader(address));

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);

    kvstore::SetRequest set_req;
    set_req.set_key("alpha");
    set_req.set_value("beta");
    kvstore::SetResponse set_resp;
    grpc::ClientContext set_ctx;
    auto set_status = stub.Set(&set_ctx, set_req, &set_resp);
    ASSERT_TRUE(set_status.ok());
    ASSERT_TRUE(set_resp.success());

    kvstore::GetRequest get_req;
    get_req.set_key("alpha");
    kvstore::GetResponse get_resp;
    grpc::ClientContext get_ctx;
    auto get_status = stub.Get(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(get_status.ok());
    ASSERT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "beta");

}

TEST(RaftIntegration, DeleteRemovesKey) {
    const std::string port = "18052";
    const std::string address = "localhost:" + port;
    TestServer server(port);
    ASSERT_GT(server.pid, 0);
    ASSERT_TRUE(WaitForServer(address));
    ASSERT_TRUE(WaitForLeader(address));

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);

    kvstore::SetRequest set_req;
    set_req.set_key("gamma");
    set_req.set_value("delta");
    kvstore::SetResponse set_resp;
    grpc::ClientContext set_ctx;
    auto set_status = stub.Set(&set_ctx, set_req, &set_resp);
    ASSERT_TRUE(set_status.ok());
    ASSERT_TRUE(set_resp.success());

    kvstore::DeleteRequest del_req;
    del_req.set_key("gamma");
    kvstore::DeleteResponse del_resp;
    grpc::ClientContext del_ctx;
    auto del_status = stub.Delete(&del_ctx, del_req, &del_resp);
    ASSERT_TRUE(del_status.ok());
    ASSERT_TRUE(del_resp.success());

    kvstore::GetRequest get_req;
    get_req.set_key("gamma");
    kvstore::GetResponse get_resp;
    grpc::ClientContext get_ctx;
    auto get_status = stub.Get(&get_ctx, get_req, &get_resp);
    ASSERT_TRUE(get_status.ok());
    EXPECT_FALSE(get_resp.found());

}

TEST(RaftIntegration, ListsKeysAndReportsClusterStatus) {
    const std::string port = "18053";
    const std::string address = "localhost:" + port;
    TestServer server(port);
    ASSERT_GT(server.pid, 0);
    ASSERT_TRUE(WaitForServer(address));
    ASSERT_TRUE(WaitForLeader(address));

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);

    for (const auto& entry : {std::pair{"first", "one"}, std::pair{"second", "two"}}) {
        kvstore::SetRequest set_req;
        set_req.set_key(entry.first);
        set_req.set_value(entry.second);
        kvstore::SetResponse set_resp;
        grpc::ClientContext set_ctx;
        const auto set_status = stub.Set(&set_ctx, set_req, &set_resp);
        ASSERT_TRUE(set_status.ok());
        ASSERT_TRUE(set_resp.success());
    }

    kvstore::ListKeysRequest list_req;
    kvstore::ListKeysResponse list_resp;
    grpc::ClientContext list_ctx;
    const auto list_status = stub.ListKeys(&list_ctx, list_req, &list_resp);
    ASSERT_TRUE(list_status.ok());
    ASSERT_EQ(list_resp.keys_size(), 2);
    EXPECT_EQ(list_resp.leader_id(), std::stoi(port));
    EXPECT_NE(std::find(list_resp.keys().begin(), list_resp.keys().end(), "first"), list_resp.keys().end());
    EXPECT_NE(std::find(list_resp.keys().begin(), list_resp.keys().end(), "second"), list_resp.keys().end());

    kvstore::ClusterStatusRequest status_req;
    kvstore::ClusterStatusResponse status_resp;
    grpc::ClientContext status_ctx;
    const auto status = stub.GetClusterStatus(&status_ctx, status_req, &status_resp);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(status_resp.node_id(), std::stoi(port));
    EXPECT_EQ(status_resp.leader_id(), std::stoi(port));
    EXPECT_GE(status_resp.current_term(), 0);
    EXPECT_GE(status_resp.commit_index(), 2);
    EXPECT_TRUE(status_resp.is_leader());

}

TEST(RaftIntegration, RecoversCommittedValueAfterRestart) {
    const std::string port = "18054";
    const std::string address = "localhost:" + port;

    {
        TestServer server(port);
        ASSERT_GT(server.pid, 0);
        ASSERT_TRUE(WaitForServer(address));
        ASSERT_TRUE(WaitForLeader(address));

        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::SetRequest request;
        request.set_key("persistent-key");
        request.set_value("persistent-value");
        kvstore::SetResponse response;
        grpc::ClientContext context;
        ASSERT_TRUE(stub.Set(&context, request, &response).ok());
        ASSERT_TRUE(response.success());
    }

    TestServer restarted(port, false);
    ASSERT_GT(restarted.pid, 0);
    ASSERT_TRUE(WaitForServer(address));
    ASSERT_TRUE(WaitForLeader(address));

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);
    kvstore::GetRequest request;
    request.set_key("persistent-key");
    kvstore::GetResponse response;
    grpc::ClientContext context;
    ASSERT_TRUE(stub.Get(&context, request, &response).ok());
    ASSERT_TRUE(response.found());
    EXPECT_EQ(response.value(), "persistent-value");
}

TEST(RaftIntegration, FollowerForwardsWritesToLeader) {
    const std::string leader_port = "18055";
    const std::string follower_port = "18056";
    const std::string third_port = "18057";

    auto leader_address = "localhost:" + leader_port;
    auto follower_address = "localhost:" + follower_port;

    pid_t leader_pid = StartServer(leader_port, {"localhost:18056", "localhost:18057"});
    pid_t follower_pid = StartServer(follower_port, {"localhost:18055", "localhost:18057"});
    pid_t third_pid = StartServer(third_port, {"localhost:18055", "localhost:18056"});

    ASSERT_GT(leader_pid, 0);
    ASSERT_GT(follower_pid, 0);
    ASSERT_GT(third_pid, 0);

    ASSERT_TRUE(WaitForServer(leader_address));
    ASSERT_TRUE(WaitForServer(follower_address));
    ASSERT_TRUE(WaitForServer("localhost:" + third_port));

    std::string leader_seen;
    for (int attempt = 0; attempt < 50; ++attempt) {
        for (const auto &address : {leader_address, follower_address, "localhost:" + third_port}) {
            auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
            kvstore::KVStore::Stub stub(channel);
            kvstore::ClusterStatusRequest status_req;
            kvstore::ClusterStatusResponse status_resp;
            grpc::ClientContext status_ctx;
            status_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
            const auto status = stub.GetClusterStatus(&status_ctx, status_req, &status_resp);
            if (status.ok() && status_resp.is_leader()) {
                leader_seen = address;
                break;
            }
        }
        if (!leader_seen.empty()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_FALSE(leader_seen.empty());

    const auto follower_address_without_leader = leader_seen == leader_address ? follower_address : leader_address;
    auto channel = grpc::CreateChannel(follower_address_without_leader, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);

    kvstore::SetRequest request;
    request.set_key("forwarded-from-follower");
    request.set_value("ok");
    kvstore::SetResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    ASSERT_TRUE(stub.Set(&context, request, &response).ok());
    ASSERT_TRUE(response.success());

    auto leader_channel = grpc::CreateChannel(leader_seen, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub leader_stub(leader_channel);
    kvstore::GetRequest get_req;
    get_req.set_key("forwarded-from-follower");
    kvstore::GetResponse get_resp;
    grpc::ClientContext get_ctx;
    get_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    ASSERT_TRUE(leader_stub.Get(&get_ctx, get_req, &get_resp).ok());
    ASSERT_TRUE(get_resp.found());
    EXPECT_EQ(get_resp.value(), "ok");

    kill(leader_pid, SIGTERM);
    kill(follower_pid, SIGTERM);
    kill(third_pid, SIGTERM);
    waitpid(leader_pid, nullptr, 0);
    waitpid(follower_pid, nullptr, 0);
    waitpid(third_pid, nullptr, 0);
}

TEST(RaftIntegration, FollowerRepliesIncludeLeaderHint) {
    const std::vector<std::string> ports = {"18066", "18067", "18068"};
    for (const auto& port : ports) {
        std::error_code error;
        std::filesystem::remove_all(std::filesystem::path("data") / ("node_" + port), error);
    }

    TestCluster cluster(ports);
    for (const auto& port : ports) {
        ASSERT_TRUE(WaitForServer("localhost:" + port));
    }
    ASSERT_TRUE(WaitForLeader("localhost:" + ports.front()));

    std::string leader_port;
    std::string follower_port;
    for (const auto& port : ports) {
        auto channel = grpc::CreateChannel("localhost:" + port, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::ClusterStatusRequest request;
        kvstore::ClusterStatusResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        ASSERT_TRUE(stub.GetClusterStatus(&context, request, &response).ok());
        if (response.is_leader()) {
            leader_port = port;
        } else if (follower_port.empty()) {
            follower_port = port;
        }
    }
    ASSERT_FALSE(leader_port.empty());
    ASSERT_FALSE(follower_port.empty());

    auto follower_channel = grpc::CreateChannel("localhost:" + follower_port, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub follower_stub(follower_channel);

    kvstore::GetRequest get_request;
    get_request.set_key("leader-hint-read");
    kvstore::GetResponse get_response;
    grpc::ClientContext get_context;
    ASSERT_TRUE(follower_stub.Get(&get_context, get_request, &get_response).ok());
    EXPECT_EQ(get_response.leader_id(), std::stoi(leader_port));

    kvstore::SetRequest set_request;
    set_request.set_key("leader-hint-write");
    set_request.set_value("value");
    kvstore::SetResponse set_response;
    grpc::ClientContext set_context;
    ASSERT_TRUE(follower_stub.Set(&set_context, set_request, &set_response).ok());
    EXPECT_TRUE(set_response.success());
    EXPECT_EQ(set_response.leader_id(), std::stoi(leader_port));
}

TEST(RaftIntegration, DuplicateWriteRequestsWithSameIdAreIdempotent) {
    const std::string port = "18069";
    const std::string address = "localhost:" + port;
    TestServer server(port);
    ASSERT_GT(server.pid, 0);
    ASSERT_TRUE(WaitForServer(address));
    ASSERT_TRUE(WaitForLeader(address));

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);

    kvstore::SetRequest first;
    first.set_key("idempotent-key");
    first.set_value("first-value");
    first.set_request_id("retry-1");
    kvstore::SetResponse first_response;
    grpc::ClientContext first_context;
    ASSERT_TRUE(stub.Set(&first_context, first, &first_response).ok());
    ASSERT_TRUE(first_response.success());

    kvstore::SetRequest duplicate;
    duplicate.set_key("idempotent-key");
    duplicate.set_value("second-value");
    duplicate.set_request_id("retry-1");
    kvstore::SetResponse duplicate_response;
    grpc::ClientContext duplicate_context;
    ASSERT_TRUE(stub.Set(&duplicate_context, duplicate, &duplicate_response).ok());
    ASSERT_TRUE(duplicate_response.success());

    kvstore::GetRequest get_request;
    get_request.set_key("idempotent-key");
    kvstore::GetResponse get_response;
    grpc::ClientContext get_context;
    ASSERT_TRUE(stub.Get(&get_context, get_request, &get_response).ok());
    ASSERT_TRUE(get_response.found());
    EXPECT_EQ(get_response.value(), "first-value");
}

TEST(RaftIntegration, RejectsVoteFromCandidateWithStaleLog) {
    const std::vector<std::string> ports = {"18060", "18061", "18062"};
    for (const auto& port : ports) {
        std::error_code error;
        std::filesystem::remove_all(std::filesystem::path("data") / ("node_" + port), error);
    }
    TestCluster cluster(ports);
    for (const auto& port : ports) {
        ASSERT_TRUE(WaitForServer("localhost:" + port));
    }
    ASSERT_TRUE(WaitForLeader("localhost:" + ports.front()));

    std::string voter_address;
    kvstore::ClusterStatusResponse voter_status;
    for (const auto& port : ports) {
        const auto address = "localhost:" + port;
        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::ClusterStatusRequest status_request;
        kvstore::ClusterStatusResponse status_response;
        grpc::ClientContext status_context;
        ASSERT_TRUE(stub.GetClusterStatus(&status_context, status_request, &status_response).ok());
        if (!status_response.is_leader()) {
            voter_address = address;
            voter_status = status_response;
            break;
        }
    }
    ASSERT_FALSE(voter_address.empty());
    ASSERT_GE(voter_status.commit_index(), 0);

    auto voter_channel = grpc::CreateChannel(voter_address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub voter_stub(voter_channel);
    kvstore::VoteRequest vote_request;
    vote_request.set_candidate_term(voter_status.current_term() + 1);
    vote_request.set_candidate_id(18063);
    vote_request.set_candidate_last_log_index(-1);
    vote_request.set_candidate_last_log_term(0);
    kvstore::VoteResponse vote_response;
    grpc::ClientContext vote_context;
    ASSERT_TRUE(voter_stub.RequestVote(&vote_context, vote_request, &vote_response).ok());
    EXPECT_FALSE(vote_response.vote_granted());
}

TEST(RaftIntegration, FollowersReportCurrentLeaderId) {
    const std::vector<std::string> ports = {"18063", "18064", "18065"};
    for (const auto& port : ports) {
        std::error_code error;
        std::filesystem::remove_all(std::filesystem::path("data") / ("node_" + port), error);
    }

    TestCluster cluster(ports);
    for (const auto& port : ports) {
        ASSERT_TRUE(WaitForServer("localhost:" + port));
    }
    ASSERT_TRUE(WaitForLeader("localhost:" + ports.front()));

    std::string leader_port;
    std::vector<std::string> follower_ports;
    for (const auto& port : ports) {
        auto channel = grpc::CreateChannel("localhost:" + port, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::ClusterStatusRequest request;
        kvstore::ClusterStatusResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        ASSERT_TRUE(stub.GetClusterStatus(&context, request, &response).ok());
        if (response.is_leader()) {
            leader_port = port;
        } else {
            follower_ports.push_back(port);
        }
    }
    ASSERT_FALSE(leader_port.empty());
    ASSERT_FALSE(follower_ports.empty());

    for (const auto& port : follower_ports) {
        auto channel = grpc::CreateChannel("localhost:" + port, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::ClusterStatusRequest request;
        kvstore::ClusterStatusResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        ASSERT_TRUE(stub.GetClusterStatus(&context, request, &response).ok());
        EXPECT_EQ(response.leader_id(), std::stoi(leader_port));
        EXPECT_FALSE(response.is_leader());
    }
}

TEST(RaftIntegration, LeaderStepsDownWhenItLosesQuorum) {
    const std::vector<std::string> ports = {"18064", "18065", "18066"};
    for (const auto& port : ports) {
        std::error_code error;
        std::filesystem::remove_all(std::filesystem::path("data") / ("node_" + port), error);
    }

    std::vector<pid_t> pids;
    for (const auto& port : ports) {
        std::vector<std::string> peers;
        for (const auto& peer_port : ports) {
            if (peer_port != port) {
                peers.push_back("localhost:" + peer_port);
            }
        }
        pids.push_back(StartServer(port, peers));
    }

    for (const auto& port : ports) {
        ASSERT_TRUE(WaitForServer("localhost:" + port));
    }

    std::string leader_address;
    for (const auto& port : ports) {
        auto channel = grpc::CreateChannel("localhost:" + port, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::ClusterStatusRequest request;
        kvstore::ClusterStatusResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        if (stub.GetClusterStatus(&context, request, &response).ok() && response.is_leader()) {
            leader_address = "localhost:" + port;
            break;
        }
    }
    ASSERT_FALSE(leader_address.empty());

    std::string leader_port = leader_address.substr(leader_address.find(':') + 1);
    std::vector<std::string> followers;
    for (const auto& port : ports) {
        if (port != leader_port) {
            followers.push_back(port);
        }
    }
    ASSERT_EQ(followers.size(), 2);

    for (const auto& follower_port : followers) {
        const auto follower_pid = std::stoi(follower_port) == 18064 ? pids[0] :
                                 (std::stoi(follower_port) == 18065 ? pids[1] : pids[2]);
        kill(follower_pid, SIGTERM);
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        auto channel = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::ClusterStatusRequest request;
        kvstore::ClusterStatusResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        if (stub.GetClusterStatus(&context, request, &response).ok() && !response.is_leader()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    auto channel = grpc::CreateChannel(leader_address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);
    kvstore::ClusterStatusRequest request;
    kvstore::ClusterStatusResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    ASSERT_TRUE(stub.GetClusterStatus(&context, request, &response).ok());
    EXPECT_FALSE(response.is_leader());

    for (const auto pid : pids) {
        if (pid > 0) {
            kill(pid, SIGTERM);
            waitpid(pid, nullptr, 0);
        }
    }
}

TEST(RaftIntegration, EmptyAppendEntriesPreservesMatchingSuffix) {
    const std::string port = "18059";
    const std::string address = "localhost:" + port;
    TestServer server(port);
    ASSERT_GT(server.pid, 0);
    ASSERT_TRUE(WaitForServer(address));
    ASSERT_TRUE(WaitForLeader(address));

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    kvstore::KVStore::Stub stub(channel);
    for (const auto& key : {"heartbeat-prefix", "heartbeat-suffix"}) {
        kvstore::SetRequest set_request;
        set_request.set_key(key);
        set_request.set_value("value");
        kvstore::SetResponse set_response;
        grpc::ClientContext set_context;
        ASSERT_TRUE(stub.Set(&set_context, set_request, &set_response).ok());
        ASSERT_TRUE(set_response.success());
    }

    kvstore::ClusterStatusRequest cluster_request;
    kvstore::ClusterStatusResponse cluster_response;
    grpc::ClientContext cluster_context;
    ASSERT_TRUE(stub.GetClusterStatus(&cluster_context, cluster_request, &cluster_response).ok());

    kvstore::AppendEntriesRequest append_request;
    append_request.set_leader_term(cluster_response.current_term());
    append_request.set_leader_id(std::stoi(port));
    append_request.set_prev_log_index(cluster_response.commit_index() - 1);
    append_request.set_prev_log_term(cluster_response.current_term());
    kvstore::AppendEntriesResponse append_response;
    grpc::ClientContext append_context;
    ASSERT_TRUE(stub.AppendEntries(&append_context, append_request, &append_response).ok());
    ASSERT_TRUE(append_response.success());
    EXPECT_EQ(append_response.match_index(), cluster_response.commit_index());
}

TEST(RaftIntegration, CompactsCommittedLogIntoSnapshot) {
    const std::string port = "18058";
    const std::string address = "localhost:" + port;
    const auto data_path = std::filesystem::path("data") / ("node_" + port);

    {
        TestServer server(port);
        ASSERT_GT(server.pid, 0);
        ASSERT_TRUE(WaitForServer(address));
        ASSERT_TRUE(WaitForLeader(address));

        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        auto stub = kvstore::KVStore::NewStub(channel);
        for (int index = 0; index < 101; ++index) {
            kvstore::SetRequest request;
            request.set_key("snapshot-" + std::to_string(index));
            request.set_value("value");
            kvstore::SetResponse response;
            grpc::ClientContext context;
            ASSERT_TRUE(stub->Set(&context, request, &response).ok());
            ASSERT_TRUE(response.success());
        }
    }

    EXPECT_TRUE(std::filesystem::exists(data_path / "raft.snapshot"));
}

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
