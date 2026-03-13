# Samsa 🪳

Samsa (named after Gregor Samsa) is a high-performance Kafka clone written in Go. It implements the Kafka wire protocol from scratch, supporting key features like KRaft-based metadata discovery, topic partitioning, and message persistence.

## 🚀 Overview

Samsa is designed to be a lightweight but technically accurate implementation of the Kafka protocol. It demonstrates how to handle complex binary protocols, manage on-disk log structures, and build a symmetrical protocol layer that serves both a broker and a native CLI client.

## 🏗️ Architecture

The project follows a clean, decoupled architecture:

### 1. Symmetric Protocol Layer (`internal/protocol`)
Contrary to many implementations, Samsa uses a single source of truth for the Kafka protocol. Any data structure defined here automatically supports:
- **Server Mode**: Decoding Requests / Encoding Responses.
- **Client Mode**: Encoding Requests / Decoding Responses.
- **Shared Primitives**: Custom `Reader` and `Writer` for Kafka's binary types (Compact Strings, Varints, etc.).

### 2. Network Engine (`internal/network`)
- **Broker**: A high-concurrency TCP server that handles request framing and dispatches to logic handlers.
- **Client**: A robust TCP dialer that manages the low-level request-response lifecycle.

### 3. Logic & Storage (`internal/handlers` & `internal/metadata`)
- **Handlers**: Cleanly separated business logic for `Produce`, `Fetch`, `ApiVersions`, and `Metadata`.
- **Disk Persistence**: Follows Kafka's log segment format (`00000000000000000000.log`).
- **KRaft Metadata**: Parses `__cluster_metadata` logs for dynamic topic and partition discovery.

## 🛠️ Getting Started

### Prerequisites
- Go 1.22+

### Installation
```bash
git clone https://github.com/albertopastormr/samsa.git
cd samsa
go build -o server cmd/server/*.go
go build -o kcli cmd/client/*.go
```

### Running the Broker
```bash
./server /path/to/server.properties
```

### Using the Client (`kcli`)
The built-in client allows you to interact with any Kafka-compatible broker:
```bash
./kcli apiversions
./kcli metadata my-topic
./kcli produce my-topic 0 "Hello, Samsa!"
```

## 📜 Supported APIs
- **Produce** (v11)
- **Fetch** (v16)
- **ApiVersions** (v4)
- **DescribeTopicPartitions** (v0)

## ⚖️ License
MIT
