# ⚡ Mini-Redis Engine (C++17)

A lightweight, concurrent in-memory key-value caching database built from scratch in Modern C++17. Designed to emulate core primitives of Redis with an O(1) LRU cache, real-time TTL, POSIX TCP sockets, mutex-based thread safety, and Write-Ahead Logging (WAL) crash recovery.

---

## 🏗️ System Architecture

```text
       [ External TCP Clients / Applications ]
                          │
                          │ (Port 8080)
                          ▼
        ┌────────────────────────────────────┐
        │       Linux POSIX Socket Server    │
        │   socket() -> bind() -> listen()   │
        └─────────────────┬──────────────────┘
                          │ (Thread Safety)
                          ▼
        ┌────────────────────────────────────┐
        │      ThreadSafeLRUCache Engine     │
        │      (std::mutex & lock_guard)     │
        └───────┬────────────────────┬───────┘
                │                    │
                ▼                    ▼
   ┌───────────────────────┐  ┌───────────────────────┐
   │ Fast Lookups & Order  │  │   Durable Storage     │
   │ std::unordered_map    │  │  Write-Ahead Log      │
   │         +             │  │     (wal.log)         │
   │ std::list (DLL)       │  │ (Append-Only on Disk) │
   │   O(1) LRU & TTL      │  │ Automatic Crash Recon │
   └───────────────────────┘  └───────────────────────┘

✨ Key Features

    O(1) Memory Engine: Combines std::unordered_map with Doubly Linked List (std::list) for constant-time operations.
    Automatic LRU Eviction: Automatically drops the least recently used keys when memory reaches capacity.
    Time-To-Live (TTL): Monotonic clock tracking using std::chrono::steady_clock with lazy key expiration.
    Thread-Safety: Concurrency control via std::mutex and RAII-based std::lock_guard.
    POSIX TCP Networking: Full-duplex client-server socket communication over port 8080.
    Crash Recovery: Write-Ahead Logging (WAL) persists all mutations to disk and restores state on reboot.

🛠️ Supported Commands

    PING - Health check probe (Returns +PONG)
    SET <key> <value> - Inserts or updates key-value pair in O(1) time
    SETEX <key> <ttl_seconds> <value> - Inserts key-value with TTL expiration
    GET <key> - Retrieves value and moves key to MRU position
    DEL <key> - Evicts key from memory and logs deletion
    QUIT - Gracefully closes the TCP client connection

🚀 How to Compile and Run

# Compile using g++ with C++17 and pthread
g++ -std=c++17 -pthread main.cpp -o mini_redis

# Run the executable
./mini_redis

🧪 Verified Execution Output

==========================================================
    UNIFIED MINI-REDIS ENGINE (PHASES 1, 2, 3, 4 COMBINED) 
==========================================================
[Engine]: Recovered 5 operations from disk log!
[Server]: TCP Server listening on port 8080...
[Client]: Connected to Unified Engine over TCP!

[Server]: Client connected successfully!
[Client -> Server]: PING
[Server -> Client]: +PONG
[Client -> Server]: SET user:1 Kiran
[Server -> Client]: +OK
[Client -> Server]: SET user:2 Suresh
[Server -> Client]: +OK
[Client -> Server]: SETEX session_token 5 secret123
[Server -> Client]: +OK (TTL set)
[Client -> Server]: GET user:1
[Server -> Client]: Kiran
[Client -> Server]: GET session_token
[Server -> Client]: secret123
[Client -> Server]: DEL user:2
[Server -> Client]: :1 (Deleted)
[Client -> Server]: QUIT
[Server -> Client]: +BYE
[Server]: Client disconnected.

[Engine]: Current Memory State After Client Session:
  - [session_token : secret123]
  - [user:1 : Kiran]
  - [role : Architect]
  - [user:102 : Suresh]

