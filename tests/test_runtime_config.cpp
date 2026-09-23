#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "../runtime_config.h"

TEST(RuntimeConfig, ParsesValuesAndPeerLists)
{
    const auto path = std::filesystem::temp_directory_path() / "kvstore-runtime-test.conf";
    {
        std::ofstream output(path);
        output << "# node settings\n"
               << "port = 50051\n"
               << "peers = localhost:50052, localhost:50053\n"
               << "data_root = ./state\n"
               << "peers_file = peers.txt\n"
               << "ca = ca.pem\n"
               << "cert = node.pem\n"
               << "key = node.key\n";
    }

    const auto config = LoadRuntimeConfig(path);
    EXPECT_EQ(config.port, "50051");
    ASSERT_EQ(config.peers.size(), 2U);
    EXPECT_EQ(config.peers[0], "localhost:50052");
    EXPECT_EQ(config.peers[1], "localhost:50053");
    EXPECT_EQ(config.data_root, "./state");
    EXPECT_EQ(config.peers_file, "peers.txt");
    EXPECT_EQ(config.ca_file, "ca.pem");
    EXPECT_EQ(config.certificate_file, "node.pem");
    EXPECT_EQ(config.private_key_file, "node.key");
    std::filesystem::remove(path);
}

TEST(RuntimeConfig, RejectsUnknownKeys)
{
    const auto path = std::filesystem::temp_directory_path() / "kvstore-runtime-invalid.conf";
    {
        std::ofstream output(path);
        output << "unsupported = value\n";
    }
    EXPECT_THROW(LoadRuntimeConfig(path), std::runtime_error);
    std::filesystem::remove(path);
}
