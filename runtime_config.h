#pragma once

#include <filesystem>
#include <string>
#include <vector>

struct RuntimeConfig {
    std::string port;
    std::string bind_host = "127.0.0.1";
    std::vector<std::string> peers;
    std::filesystem::path data_root = "data";
    std::filesystem::path peers_file;
    std::string ca_file;
    std::string certificate_file;
    std::string private_key_file;
};

RuntimeConfig LoadRuntimeConfig(const std::filesystem::path &path);
bool IsLoopbackBindHost(const std::string &host);
void ValidateListenerSecurity(const std::string &bind_host,
                              bool tls_enabled,
                              bool any_tls_option);
