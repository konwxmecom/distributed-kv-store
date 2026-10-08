#include <chrono>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <stdexcept>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

using grpc::ClientContext;
using grpc::Status;
using grpc::Channel;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::KVStore;
using kvstore::SetRequest;
using kvstore::SetResponse;

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

// A thin wrapper around the generated gRPC stub, exposing simple
// Set()/Get() methods for interacting with the KV store cluster.
class KVStoreClient
{
private:
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<KVStore::Stub> stub_;
    std::string current_address_;
    std::shared_ptr<grpc::ChannelCredentials> credentials_;
    std::vector<std::string> addresses_;
    std::string client_id_;
    std::uint64_t request_sequence_ = 0;

    template <typename Request, typename Response>
    bool TryCall(const std::string &method_name,
                 const Request &request,
                 Response *response,
                 std::function<Status(grpc::ClientContext *, const Request &, Response *)> call)
    {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        Status status = call(&context, request, response);
        if (status.ok())
            return true;

        std::cout << method_name << " failed: " << status.error_message() << std::endl;
        return false;
    }

    void ReconnectToLeader(int leader_port)
    {
        std::string next_address = "localhost:" + std::to_string(leader_port);
        for (const auto &address : addresses_)
        {
            const auto separator = address.rfind(':');
            if (separator != std::string::npos &&
                address.substr(separator + 1) == std::to_string(leader_port))
            {
                next_address = address;
                break;
            }
        }
        ReconnectToAddress(next_address);
    }

    void ReconnectToAddress(const std::string &next_address)
    {
        if (next_address == current_address_)
            return;
        current_address_ = next_address;
        channel_ = grpc::CreateChannel(next_address, credentials_);
        stub_ = KVStore::NewStub(channel_);
    }

    void ReconnectToNextAddress()
    {
        const auto current = std::find(addresses_.begin(), addresses_.end(), current_address_);
        if (current == addresses_.end() || addresses_.size() < 2)
            return;
        const auto next = std::next(current) == addresses_.end() ? addresses_.begin() : std::next(current);
        ReconnectToAddress(*next);
    }

public:
    explicit KVStoreClient(std::vector<std::string> addresses,
                           std::shared_ptr<grpc::ChannelCredentials> credentials)
        : credentials_(std::move(credentials)), addresses_(std::move(addresses))
    {
        if (addresses_.empty())
            throw std::invalid_argument("at least one server address is required");
        std::random_device random;
        client_id_ = "client-" + std::to_string(random()) + "-" + std::to_string(random());
        for (const auto &address : addresses_)
        {
            auto candidate_channel = grpc::CreateChannel(address, credentials_);
            auto candidate_stub = KVStore::NewStub(candidate_channel);
            kvstore::ClusterStatusRequest request;
            kvstore::ClusterStatusResponse response;
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
            if (candidate_stub->GetClusterStatus(&context, request, &response).ok() && response.is_leader())
            {
                current_address_ = address;
                channel_ = std::move(candidate_channel);
                stub_ = std::move(candidate_stub);
                return;
            }
        }
        current_address_ = addresses_.front();
        channel_ = grpc::CreateChannel(current_address_, credentials_);
        stub_ = KVStore::NewStub(channel_);
    }

    bool Set(const std::string &key, const std::string &value)
    {
        const std::string request_id = "client-set-" +
            std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        const std::uint64_t request_sequence = ++request_sequence_;

        for (int attempt = 0; attempt < 3; ++attempt)
        {
            SetRequest request;
            request.set_key(key);
            request.set_value(value);
            request.set_request_id(request_id);
            request.set_client_id(client_id_);
            request.set_request_sequence(request_sequence);

            SetResponse response;
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            const Status status = stub_->Set(&context, request, &response);
            if (status.ok())
            {
                if (response.success())
                    return true;
                if (response.leader_id() > 0)
                {
                    const auto previous_address = current_address_;
                    ReconnectToLeader(response.leader_id());
                    if (current_address_ != previous_address)
                        continue;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            std::cout << "Set failed: " << status.error_message() << std::endl;
            ReconnectToNextAddress();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    bool Get(const std::string &key, std::string &value_out)
    {
        GetRequest request;
        request.set_key(key);
        for (int attempt = 0; attempt < 3; ++attempt)
        {
            GetResponse response;
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            const Status status = stub_->Get(&context, request, &response);
            if (status.ok())
            {
                if (response.leader_id() > 0)
                {
                    const auto previous_address = current_address_;
                    ReconnectToLeader(response.leader_id());
                    if (current_address_ != previous_address)
                        continue;
                }
                if (response.found())
                {
                    value_out = response.value();
                    return true;
                }
                return false;
            }
            std::cout << "Get failed: " << status.error_message() << std::endl;
            ReconnectToNextAddress();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        return false;
    }
};

int main(int argc, char **argv)
{
    TlsConfig tls_config;
    std::vector<std::string> addresses = {"localhost:50051"};
    for (int i = 1; i < argc; ++i)
    {
        if (std::string(argv[i]) == "--address" && i + 1 < argc)
            addresses = {argv[++i]};
        else if (std::string(argv[i]) == "--leader" && i + 1 < argc)
            addresses = {argv[++i]};
        else if (std::string(argv[i]) == "--fallback" && i + 1 < argc)
            addresses.push_back(argv[++i]);
        else if (std::string(argv[i]) == "--ca" && i + 1 < argc)
            tls_config.ca_file = argv[++i];
        else if (std::string(argv[i]) == "--cert" && i + 1 < argc)
            tls_config.certificate_file = argv[++i];
        else if (std::string(argv[i]) == "--key" && i + 1 < argc)
            tls_config.private_key_file = argv[++i];
    }

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

    KVStoreClient client(addresses, credentials);

    // Basic smoke test: write a key, read it back, and confirm a missing
    // key correctly reports "not found".
    bool set_ok = client.Set("name", "Prayagraj");
    std::cout << "SET result: " << (set_ok ? "success" : "failed") << std::endl;

    std::string value;
    bool found = client.Get("name", value);
    if (found)
    {
        std::cout << "GET result: " << value << std::endl;
    }
    else
    {
        std::cout << "GET result: key not found" << std::endl;
    }

    std::string missing_value;
    bool missing_found = client.Get("doesnotexist", missing_value);
    std::cout << "GET (missing key) result: " << (missing_found ? missing_value : "not found") << std::endl;

    return 0;
}