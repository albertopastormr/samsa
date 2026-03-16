# Samsa

[![Go Report Card](https://goreportcard.com/badge/github.com/albertopastormr/samsa)](https://goreportcard.com/report/github.com/albertopastormr/samsa)
[![Build Status](https://github.com/albertopastormr/samsa/actions/workflows/ci.yml/badge.svg)](https://github.com/albertopastormr/samsa/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Samsa** is a lightweight, Kafka-compatible message broker and CLI written in Go. It implements the core Kafka binary protocol, providing a high-performance distribution system for event-driven architectures.

## 🚀 Features

- **Single Binary Architecture**: The `samsa` CLI serves as both the broker (server) and the administrative tool (client).
- **Kafka Compatibility**: Implements key Kafka APIs including `Produce`, `Fetch`, `ApiVersions`, and `DescribeTopicPartitions`.
- **Disk Persistence**: High-throughput message logging with persistent storage on disk.
- **Custom Protocol Engine**: Hand-crafted binary decoding and encoding for maximum efficiency.
- **Graceful Shutdown**: Robust lifecycle management ensures zero data corruption during server restarts by flushing active writes and completing inflight requests.

## 📦 Installation

**Download pre-compiled binaries:**
You can download the latest version of Samsa for Linux, macOS, or Windows from the [Releases page](https://github.com/albertopastormr/samsa/releases).


## 🛠️ Quick Start

### 1. Build
Compile the project into a single executable using the provided `Makefile`:
```bash
make build
```
This creates the `samsa` binary in the `bin/` directory.

### 2. Start the Server
Run the broker on the default port (`9092`):
```bash
./bin/samsa server
```

### 3. Produce and Consume
In separate terminals, you can use the built-in client to interact with the broker:

**List Topics:**
```bash
./bin/samsa topic list
```

**Describe a Topic:**
```bash
./bin/samsa topic describe --name my-topic
```

**Produce a Message:**
```bash
./bin/samsa produce --topic my-topic --message "Hello Samsa"
```

**Fetch Messages:**
*(Note: Use the topic ID returned by the `topic list` or `topic describe` command)*
```bash
./bin/samsa fetch --topic-id <topic-uuid> --partition 0
```

## 🏗️ Architecture

Samsa follows a **Hexagonal (Clean) Architecture** to ensure maintainability and testability:

- **Network Layer**: Handles TCP connection lifecycles, concurrency, and graceful shutdown via `NotifyContext`.
- **Protocol Layer**: A dedicated engine for manual bit-flipping and parsing of Kafka's binary wire format.
- **Handlers**: Decoupled logic for processing Produce, Fetch, and Metadata requests.
- **Storage Layer**: Manages thread-safe metadata and high-performance append-only logging to disk.

### System Flow Diagram

```mermaid
graph TD
    Client["Kafka Client / Samsa CLI"]
    
    subgraph Samsa ["Samsa Broker (Go Binary)"]
        direction TB
        
        subgraph Network ["Network & Lifecycle"]
            TCP["TCP Server / Listener"]
            Graceful["Shutdown Controller"]
        end
        
        subgraph Engine ["Protocol Engine"]
            Decoder["Binary Decoder"]
            Encoder["Binary Encoder"]
        end
        
        subgraph Handlers ["API Handlers"]
            Produce["Produce API"]
            Fetch["Fetch API"]
            Meta["Metadata API"]
        end
        
        subgraph Persistence ["Data & Metadata"]
            Store["Metadata Store"]
            Logs["Log Segments (*.log)"]
        end
        
        %% Internal Flow
        TCP --> Decoder
        Decoder --> Produce
        Decoder --> Fetch
        Decoder --> Meta
        
        Produce --> Logs
        Fetch --> Logs
        Meta --> Store
        
        Produce --> Encoder
        Fetch --> Encoder
        Meta --> Encoder
        Encoder --> TCP
    end

    Client <--> TCP
```

## 🧪 Development

Run the full test suite with race detection:
```bash
make test
```

Cleanup build artifacts and logs:
```bash
make clean
```

---
*Inspired by the Kafka protocol. Built for education and performance.*
