#include <iostream>
#include <memory>
#include <string>

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

// A thin wrapper around the generated gRPC stub, exposing simple
// Set()/Get() methods for interacting with the KV store cluster.
class KVStoreClient
{
private:
    std::unique_ptr<KVStore::Stub> stub_;

public:
    explicit KVStoreClient(std::shared_ptr<Channel> channel)
        : stub_(KVStore::NewStub(channel)) {}

    // Sends a Set RPC. Returns true if the write succeeded.
    bool Set(const std::string &key, const std::string &value)
    {
        SetRequest request;
        request.set_key(key);
        request.set_value(value);

        SetResponse response;
        ClientContext context;

        Status status = stub_->Set(&context, request, &response);

        if (status.ok())
        {
            return response.success();
        }
        else
        {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
            return false;
        }
    }

    // Sends a Get RPC. Returns true and populates value_out if the key exists.
    bool Get(const std::string &key, std::string &value_out)
    {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

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
        else
        {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
            return false;
        }
    }
};

int main()
{
    // Connects to the node running on port 50051. Point this at any node
    // in the cluster - reads/writes should be sent to the current leader.
    KVStoreClient client(
        grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));

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