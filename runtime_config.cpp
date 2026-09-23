#include "runtime_config.h"

#include <fstream>
#include <stdexcept>
#include <string>

namespace {

std::string Trim(const std::string &value)
{
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos)
        return {};
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::vector<std::string> SplitPeers(const std::string &value)
{
    std::vector<std::string> peers;
    std::size_t start = 0;
    while (start <= value.size())
    {
        const auto comma = value.find(',', start);
        const auto part = Trim(value.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
        if (!part.empty())
            peers.push_back(part);
        if (comma == std::string::npos)
            break;
        start = comma + 1;
    }
    return peers;
}

void Assign(RuntimeConfig &config, const std::string &key, const std::string &value)
{
    if (key == "port")
        config.port = value;
    else if (key == "peers")
        config.peers = SplitPeers(value);
    else if (key == "data_root")
        config.data_root = value;
    else if (key == "peers_file")
        config.peers_file = value;
    else if (key == "ca")
        config.ca_file = value;
    else if (key == "cert")
        config.certificate_file = value;
    else if (key == "key")
        config.private_key_file = value;
    else
        throw std::runtime_error("unknown runtime config key: " + key);
}

} // namespace

RuntimeConfig LoadRuntimeConfig(const std::filesystem::path &path)
{
    std::ifstream input(path);
    if (!input)
        throw std::runtime_error("failed to open runtime config: " + path.string());

    RuntimeConfig config;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line))
    {
        ++line_number;
        const auto comment = line.find('#');
        const auto trimmed = Trim(line.substr(0, comment));
        if (trimmed.empty() || trimmed.front() == '#')
            continue;
        const auto separator = trimmed.find('=');
        if (separator == std::string::npos)
            throw std::runtime_error("invalid runtime config at line " + std::to_string(line_number));
        const auto key = Trim(trimmed.substr(0, separator));
        const auto value = Trim(trimmed.substr(separator + 1));
        if (key.empty() || value.empty())
            throw std::runtime_error("invalid runtime config at line " + std::to_string(line_number));
        Assign(config, key, value);
    }
    const bool any_tls = !config.ca_file.empty() || !config.certificate_file.empty() || !config.private_key_file.empty();
    const bool complete_tls = !config.ca_file.empty() && !config.certificate_file.empty() && !config.private_key_file.empty();
    if (any_tls && !complete_tls)
        throw std::runtime_error("ca, cert, and key must be configured together");
    return config;
}
