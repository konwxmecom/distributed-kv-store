#include <chrono>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <vector>
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
    std::vector<double> latency_microseconds;
    latency_microseconds.reserve(static_cast<std::size_t>(operations));
    const auto started = std::chrono::steady_clock::now();
    for (int index = 0; index < operations; ++index)
    {
        kvstore::SetRequest request;
        request.set_key("benchmark-" + std::to_string(index));
        request.set_value("value");
        kvstore::SetResponse response;
        grpc::ClientContext context;
        const auto operation_started = std::chrono::steady_clock::now();
        const auto status = stub->Set(&context, request, &response);
        const auto operation_finished = std::chrono::steady_clock::now();
        if (!status.ok() || !response.success())
        {
            std::cerr << "Set failed at operation " << index << ": "
                      << status.error_message() << "\n";
            return 1;
        }
        latency_microseconds.push_back(
            std::chrono::duration<double, std::micro>(operation_finished - operation_started).count());
    }

    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::sort(latency_microseconds.begin(), latency_microseconds.end());
    const auto percentile = [&latency_microseconds](double fraction)
    {
        const auto index = static_cast<std::size_t>(
            fraction * static_cast<double>(latency_microseconds.size() - 1));
        return latency_microseconds[index];
    };
    std::cout << "operations=" << operations << " seconds=" << elapsed
              << " ops_per_second=" << (operations / elapsed)
              << " latency_us_p50=" << percentile(0.50)
              << " latency_us_p95=" << percentile(0.95)
              << " latency_us_p99=" << percentile(0.99) << '\n';
    return 0;
}
