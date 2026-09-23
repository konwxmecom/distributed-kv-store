# Setup Guide

This guide walks through setting up the full toolchain and building the project from scratch on Windows.

## Prerequisites

- Windows 10/11
- [Git](https://git-scm.com/)
- ~5 GB of free disk space (for the compiler toolchain and gRPC dependencies)

## 1. Install a C++ Compiler

Download **Build Tools for Visual Studio** from the [Visual Studio downloads page](https://visualstudio.microsoft.com/downloads/) (under "Tools for Visual Studio" — this installs just the compiler and build tools, not the full IDE).

During installation, select the **"Desktop development with C++"** workload. This provides the MSVC compiler and CMake.

Verify the install by opening **Developer PowerShell for VS** (search for it in the Start menu) and running:

```powershell
cl
cmake --version
```

Both should print version information.

## 2. Install vcpkg

vcpkg is Microsoft's C++ package manager, used here to install gRPC and its dependencies.

```powershell
mkdir C:\dev
cd C:\dev
git clone https://github.com/microsoft/vcpkg
cd vcpkg
.\bootstrap-vcpkg.bat
```

## 3. Install gRPC

```powershell
.\vcpkg install grpc
```

This builds gRPC and its dependencies (Protobuf, OpenSSL, abseil, c-ares) from source, which can take 20–40 minutes depending on your machine.

## 4. Clone This Repository

```powershell
git clone https://github.com/konwxmecom/distributed-kv-store.git
cd distributed-kv-store
```

## 5. Generated gRPC Code

The generated gRPC/Protobuf C++ files are included in this repository, so a normal
clone does not require `protoc`. Regenerate them only after changing
`proto/kvstore.proto`:

```powershell
<path-to-vcpkg>\installed\x64-windows\tools\protobuf\protoc.exe ^
  --cpp_out=. --grpc_out=. ^
  --plugin=protoc-gen-grpc=<path-to-vcpkg>\installed\x64-windows\tools\grpc\grpc_cpp_plugin.exe ^
  -I proto proto\kvstore.proto
```

Replace `<path-to-vcpkg>` with wherever you cloned vcpkg (e.g. `C:\dev\vcpkg`). This produces four files in the project root: `kvstore.pb.h`, `kvstore.pb.cc`, `kvstore.grpc.pb.h`, `kvstore.grpc.pb.cc`.

## 6. Build the Project

```powershell
mkdir build
cd build
cmake .. -DCMAKE_TOOLCHAIN_FILE=<path-to-vcpkg>\scripts\buildsystems\vcpkg.cmake
cmake --build .
cd ..
```

This produces `server.exe` and `client.exe` under `build\Debug\`.

Verify the generated executables exist before starting the cluster:

```powershell
Test-Path .\build\Debug\server.exe
Test-Path .\build\Debug\client.exe
```

## 7. Run a Cluster

Open three separate terminals and start one node in each. All nodes start symmetrically — there's no manual leader assignment; the cluster elects its own leader via Raft.

```powershell
# Terminal 1
.\build\Debug\server.exe 50051 50052 50053

# Terminal 2
.\build\Debug\server.exe 50052 50051 50053

# Terminal 3
.\build\Debug\server.exe 50053 50051 50052
```

Within a few seconds, one node will log `*** BECAME LEADER ***`.

## 8. Talk to the Cluster

In a fourth terminal:

```powershell
.\build\Debug\client.exe
```

By default the client connects to `localhost:50051`. Point it at whichever node is currently the leader (edit the address in `client.cpp` if needed).

## 9. Run the Chaos Test (Optional)

```powershell
.\chaos_test.ps1
```

This starts a fresh 3-node cluster and repeatedly kills/restarts a random node, letting you observe leader re-election and log catch-up in real time.

## 10. Persistence, TLS, and Membership

Nodes persist Raft data under `data\node_<port>\`. The WAL is replayed on startup and
committed state is periodically compacted into a snapshot.

Enable mutual TLS by passing the CA, certificate, and private key to each server and
the client:

```powershell
.\build\Debug\server.exe 50051 50052 50053 --ca ca.pem --cert node-50051.pem --key node-50051-key.pem
.\build\Debug\client.exe --address localhost:50051 --ca ca.pem --cert client.pem --key client-key.pem
```

Membership can be reloaded without restarting a node. Pass `--peers-file` and keep one
`host:port` endpoint per line; editing the file updates the peer set automatically.

## 11. Browser Console and Gateway

The browser console uses a small Python gateway to translate HTTP requests into the
existing gRPC API. Install its dependencies and run it alongside the static web server:

```powershell
python -m pip install -r requirements.txt

# Terminal 1: gateway to the C++ node
python gateway.py --target localhost:50051 --port 8080

# Terminal 2: browser UI
python -m http.server 4173 --directory web
```

Open `http://localhost:4173`. For a TLS-enabled node, add `--ca`, `--cert`, and `--key`
to the gateway command. The dashboard now reports gateway health and sends CRUD mutations
to the Raft service instead of keeping them only in browser memory.

## Troubleshooting

- **"cannot open source file grpcpp/grpcpp.h" in the editor** — this is just an IDE IntelliSense path issue and doesn't affect compilation via CMake; it resolves once you configure CMake Tools in your editor to point at the `build` folder.
- **App blocked by "Application Control" / Smart App Control** — Windows may block freshly compiled, unsigned executables. This is a Windows security feature, not a bug in the project; you may need to allow the executable or adjust this setting.
- **`cannot open ... for writing` during build** — a previous instance of `server.exe` is still running and holding the file. Close all running instances (or run `Get-Process server -ErrorAction SilentlyContinue | Stop-Process -Force`) before rebuilding.
