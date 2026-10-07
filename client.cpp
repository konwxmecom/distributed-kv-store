#include <chrono>
#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <stdexcept>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
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

    template <typename Request, typename Response>
    bool TryCall(const std::string &method_name,
                 const Request &request,
                 Response *response,
                 std::function<Status(grpc::ClientContext *, const Request &, Response *)> call)
    {
        grpc::ClientContext context;
        Status status = call(&context, request, response);
        if (status.ok())
            return true;

        std::cout << method_name << " failed: " << status.error_message() << std::endl;
        return false;
    }

    void ReconnectToLeader(int leader_port)
    {
        const auto next_address = "localhost:" + std::to_string(leader_port);
        if (next_address == current_address_)
            return;
        current_address_ = next_address;
        channel_ = grpc::CreateChannel(next_address, grpc::InsecureChannelCredentials());
        stub_ = KVStore::NewStub(channel_);
    }

public:
    explicit KVStoreClient(std::shared_ptr<Channel> channel, std::string initial_address = "")
        : channel_(std::move(channel)), stub_(KVStore::NewStub(channel_)), current_address_(std::move(initial_address))
    {
        if (current_address_.empty())
            current_address_ = "localhost:50051";
    }

    bool Set(const std::string &key, const std::string &value)
    {
        const std::string request_id = "client-set-" +
            std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());

        for (int attempt = 0; attempt < 3; ++attempt)
        {
            SetRequest request;
            request.set_key(key);
            request.set_value(value);
            request.set_request_id(request_id);

            SetResponse response;
            grpc::ClientContext context;
            const Status status = stub_->Set(&context, request, &response);
            if (status.ok())
            {
                if (response.leader_id() > 0 && response.leader_id() != std::stoi(current_address_.substr(current_address_.find(':') + 1)))
                {
                    ReconnectToLeader(response.leader_id());
                    continue;
                }
                return response.success();
            }

            std::cout << "Set failed: " << status.error_message() << std::endl;
            break;
        }
        return false;
    }

    bool Get(const std::string &key, std::string &value_out)
    {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        grpc::ClientContext context;
        Status status = stub_->Get(&context, request, &response);
        if (status.ok())
        {
            if (response.found())
            {
                value_out = response.value();
                return true;
            }
            return false;
        }

        std::cout << "Get failed: " << status.error_message() << std::endl;
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

    std::shared_ptr<grpc::Channel> channel;
    for (const std::string &address : addresses)
    {
        try
        {
            channel = grpc::CreateChannel(address, credentials);
            break;
        }
        catch (const std::exception &ex)
        {
            std::cout << "Failed to open channel to " << address << ": " << ex.what() << std::endl;
        }
    }

    if (!channel)
    {
        std::cerr << "Could not create a valid gRPC channel to any configured endpoint." << std::endl;
        return 1;
    }

    KVStoreClient client(channel);

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