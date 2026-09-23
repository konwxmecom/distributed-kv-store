cmake_minimum_required(VERSION 3.15)
project(DistributedKVStore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENABLE_UBSAN)
    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=undefined)
endif()

if(ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

if(MSVC)
    add_compile_options(/W4 /permissive-)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)

add_executable(server
    server.cpp
    runtime_config.cpp
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

add_executable(raft_tests
    tests/test_raft_integration.cpp
    kvstore.pb.cc
    kvstore.grpc.pb.cc
)

add_executable(runtime_config_tests
    tests/test_runtime_config.cpp
    runtime_config.cpp
)

target_link_libraries(raft_tests
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
    GTest::gtest_main
)

target_link_libraries(runtime_config_tests
    PRIVATE
    GTest::gtest_main
)

add_test(NAME raft_tests COMMAND raft_tests)
add_test(NAME runtime_config_tests COMMAND runtime_config_tests)

enable_testing()
