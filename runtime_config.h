#pragma once

#include <filesystem>
#include <string>
#include <vector>

struct RuntimeConfig {
    std::string port;
    std::vector<std::string> peers;
    std::filesystem::path data_root = "data";
    std::filesystem::path peers_file;
    std::string ca_file;
    std::string certificate_file;
    std::string private_key_file;
};

RuntimeConfig LoadRuntimeConfig(const std::filesystem::path &path);
