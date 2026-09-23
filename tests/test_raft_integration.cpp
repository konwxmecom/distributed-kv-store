#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
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
    std::filesystem::path root = std::filesystem::current_path();
    std::filesystem::path candidate = root / "server";
    if (std::filesystem::exists(candidate)) {
        return candidate.string();
    }
    candidate = root.parent_path() / "build" / "server";
    if (std::filesystem::exists(candidate)) {
        return candidate.string();
    }
    return "server";
}

void WaitForServer(const std::string& address, int timeout_seconds = 20) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        kvstore::KVStore::Stub stub(channel);
        kvstore::HeartbeatRequest req;
        req.set_leader_port(0);
        req.set_leader_term(0);
        kvstore::HeartbeatResponse resp;
        grpc::ClientContext context;
        auto status = stub.Heartbeat(&context, req, &resp);
        if (status.ok() && resp.alive()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    FAIL() << "server at " << address << " did not become ready";
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

TEST(RaftIntegration, LeaderSetAndGetRoundTrip) {
    const std::string port = "18051";
    const std::string address = "localhost:" + port;
    const auto pid = StartServer(port, {});
    ASSERT_GT(pid, 0);
    WaitForServer(address);

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

    StopServer(pid);
}

TEST(RaftIntegration, DeleteRemovesKey) {
    const std::string port = "18052";
    const std::string address = "localhost:" + port;
    const auto pid = StartServer(port, {});
    ASSERT_GT(pid, 0);
    WaitForServer(address);

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

    StopServer(pid);
}

}  // namespace

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
