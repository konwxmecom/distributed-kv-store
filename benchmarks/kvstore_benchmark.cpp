#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

#include <grpcpp/grpcpp.h>

#include "../kvstore.grpc.pb.h"

int main(int argc, char **argv)
{
    const std::string address = argc > 1 ? argv[1] : "localhost:50051";
    const int operations = argc > 2 ? std::atoi(argv[2]) : 1000;
    if (operations <= 0)
    {
        std::cerr << "operations must be positive\n";
        return 1;
    }

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = kvstore::KVStore::NewStub(channel);
    const auto started = std::chrono::steady_clock::now();
    for (int index = 0; index < operations; ++index)
    {
        kvstore::SetRequest request;
        request.set_key("benchmark-" + std::to_string(index));
        request.set_value("value");
        kvstore::SetResponse response;
        grpc::ClientContext context;
        const auto status = stub->Set(&context, request, &response);
        if (!status.ok() || !response.success())
        {
            std::cerr << "Set failed at operation " << index << ": "
                      << status.error_message() << "\n";
            return 1;
        }
    }

    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::cout << "operations=" << operations << " seconds=" << elapsed
              << " ops_per_second=" << (operations / elapsed) << '\n';
    return 0;
}
