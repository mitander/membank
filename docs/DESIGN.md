# CortexDB Design Decisions

This document records the high-level architectural decisions and their rationale. For detailed
specifications of individual components, see `docs/architecture/`.

## Why Zig?

The choice of Zig is fundamental to the project's goals.

1.  **Manual Memory Management:** For a database, predictable performance is critical. Avoiding a GC and having explicit control over allocations is a non-negotiable requirement for low-latency hot paths.
2.  **`comptime`:** Zig's compile-time execution allows us to build highly optimized data structures and eliminate runtime overhead. It is used extensively for configuration, serialization, and creating type-safe interfaces.
3.  **Simplicity and Readability:** Zig is a small, simple language. This reduces the cognitive load on developers and makes the entire codebase auditable. It is a "better C" that aligns with our philosophy of explicitness.
4.  **Error Handling:** Built-in, explicit error handling fits our model for building robust, fault-tolerant systems perfectly.

## Architecture: Simulation First

CortexDB is designed from the ground up to be deterministically testable. This is our primary architectural constraint.

*   The core logic is written against abstract interfaces (e.g., a "VFS" for file I/O, a "VNet" for networking).
*   We provide two implementations for these interfaces:
    1.  **Production:** Maps directly to OS system calls (`open`, `read`, `send`).
    2.  **Simulation:** Maps to in-memory data structures controlled by a single, seeded PRNG.

This allows us to write a test script like "client A sends a write, the network partitions for 2 seconds, the disk for server B corrupts a bit, then the network heals" and verify the system's behavior byte-for-byte.

## Core Primitive: The Context Block

Instead of treating context as raw text, we model it as a **Context Block**.

*   **ID:** A 128-bit unique identifier.
*   **Version:** A monotonic counter to track updates.
*   **Source URI:** The canonical source of the data (e.g., `file://...`, `git://...`).
*   **Content:** The raw data chunk.
*   **Metadata:** A flexible key-value map for `type`, `language`, `start_line`, etc.
*   **Edges:** A list of relationships to other blocks (e.g., `{type: "IMPORTS", target_id: 0x...}`).

This structure allows the query engine to perform much more intelligent retrieval than simple vector similarity searches.

## Trade-Offs

*   **Not a General-Purpose Vector DB:** While we may integrate semantic search, CortexDB is not designed to compete with specialized vector databases. Our focus is on structured, graph-based context retrieval.
*   **Write Latency vs. Consistency:** We use a standard replication model (like Viewstamped Replication) that requires a quorum for writes. This increases write latency compared to eventually consistent systems, but it is a deliberate choice to guarantee data integrity.
