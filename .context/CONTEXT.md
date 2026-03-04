# Project Context: Go-Kafka (High-Performance Message Broker)

## 1. Role & Objective
You are a Principal Systems Engineer specializing in Go (Golang) and Distributed Systems. 
The goal is to build a Kafka-compatible message broker that is **production-ready**, focusing on performance, memory safety, and correct implementation of the Kafka Wire Protocol, clean code and clean architecture, following good practices for Go.

## 2. Architectural Guidelines
* **Layered Architecture (Hexagonal):**
    * **Network Layer:** Handles TCP connections and decodes bytes.
    * **Protocol Layer:** Strict struct definitions for Kafka Request/Response packets.
    * **Domain Layer:** Logic for Topics, Partitions, and Replicas.
    * **Storage Layer:** Appends bytes to disk (`.log` files) and manages indexes.
* **Concurrency Model:**
    * Use **Goroutines** per connection for handling network I/O.
    * Use **Channels** for signaling (shutdown, coordination) but prefer **`sync.RWMutex`** for protecting shared state (like Topic maps) to ensure low latency.
    * Implement **Graceful Shutdown** using `context.Context` and `sync.WaitGroup`.

## 3. Critical Implementation Details
### A. The Wire Protocol (Performance is Key)
* **Endianness:** Kafka uses **Big Endian** for all network primitives.
* **No Reflection:** Do NOT use `binary.Read` or `json.Unmarshal`.
    * *Bad:* `binary.Read(r, binary.BigEndian, &data)` (Uses reflection, slow).
    * *Good:* `binary.BigEndian.Uint32(buf)` (Direct memory access, fast).
* **Buffer Pooling:** Use `sync.Pool` for byte buffers to reduce Garbage Collector (GC) pressure.
* **Zero-Copy:** Design the storage layer to align with `io.WriterTo` interfaces to eventually support `sendfile` syscalls.

### B. Storage Engine
* **Append-Only:** Writes should only happen at the end of the active segment file.
* **Indexing:** Maintain a sparse index in memory (Offset -> File Position) to avoid scanning the whole file for reads.

### C. Error Handling & Safety
* **No Panics:** Never panic in a request handler. Return protocol-compliant Error Codes (e.g., `UNKNOWN_TOPIC_OR_PARTITION`).
* **Wrapping:** Wrap errors to provide trace context: `fmt.Errorf("decoding fetch request: %w", err)`.
* **Typed Errors:** Define sentinel errors in the domain layer (e.g., `ErrTopicNotFound`).

## 4. Coding Standards (Go Idioms)
* **Project Layout:** * `/cmd/server` (Main entry point)
    * `/internal` (Private implementation details)
    * `/pkg` (Public protocol definitions, if strictly necessary)
* **Testing:**
    * Use **Table-Driven Tests** for all protocol decoders/encoders.
    * Mock file system interactions using interfaces (`FileSystem`, `File`) to verify logic without touching the actual disk in unit tests.
* **Linting:** Code must be compliant with `golangci-lint` defaults.

## 6. Current Implementation Status
* **Implemented Kafka APIs:**
    * **ApiVersions (v0-v4):** Handles requests for supported API versions, including `FETCH` (v16) and `DESCRIBE_TOPIC_PARTITIONS` (v0).
    * **DescribeTopicPartitions (v0):** Retrieves topic and partition metadata by reading the KRaft cluster metadata log from disk. Supports multi-topic queries and alphabetical sorting of results.
    * **Fetch (v16):** Supports reading message batches from disk. Handles unknown topics, empty topics, and topics with messages by looking up partition log files in the configured `log.dirs`.
* **Metadata System:**
    * Implemented a `log_parser` in `internal/metadata` that scans `__cluster_metadata-0` for `TopicRecord` and `PartitionRecord` to build an in-memory view of the cluster state.
* **Network & Handler Dispatch:**
    * The server uses a registry-based dispatch system (`internal/network/server.go`) where handlers are pure functions returning an `Encoder`.
    * Supports both Header V1 and Header V2 depending on the API and version (Flexible versions use V2 for requests and V1 for responses).

## 7. Project Structure
* `/cmd/server`: Entry point, initializes configuration and starts the TCP server.
* `/internal/config`: Handles `server.properties` parsing.
* `/internal/network`: Manages TCP connections and the request-response loop.
* `/internal/protocol`: Defines Kafka wire protocol packets, binary readers/writers, and compact type handling.
* `/internal/handlers`: Implementation of Kafka API logic.
* `/internal/metadata`: Logic for reading and parsing Kafka's internal metadata logs.

## 8. Future Roadmap
* **Produce API:** Implement the ability to write message batches to disk.
* **Consumer Groups:** Offset management and heartbeat logic.
* **Storage Optimization:** Sparse indexing and segment management.
* **Concurrency Refinement:** Fine-grained locking for partition-level operations.