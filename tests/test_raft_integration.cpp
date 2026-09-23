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
