# Benchmarks

The benchmark client measures end-to-end `Set` throughput against a running node.
It intentionally measures the full gRPC and Raft commit path rather than an in-process
mock, so results include network serialization and quorum latency.

Build with CMake, start a node, then run:

```text
./build/kvstore_benchmark localhost:50051 1000
```
