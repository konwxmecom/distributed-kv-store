cmake_minimum_required(VERSION 3.15)
project(DistributedKVStore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(gRPC CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)

add_executable(server
    server.cpp
    kvstore.pb.cc
    kvstore.grpc.pb.cc
)

target_link_libraries(server
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
)
add_executable(client
    client.cpp
    kvstore.pb.cc
    kvstore.grpc.pb.cc
)

target_link_libraries(client
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
)
 