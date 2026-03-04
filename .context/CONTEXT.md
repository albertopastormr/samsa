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

## 5. Specific Directives for the AI
* When generating struct definitions, verify field sizes against the **Kafka Protocol Guide**.
* Always prioritize **readability** and **explicit error handling** over clever one-liners.
* If a solution requires complex concurrency, explain the locking strategy (e.g., "We lock the partition, not the whole broker").