# CortexDB TODO

This document tracks the high-level roadmap.
Items are not strictly ordered but reflect project priorities.

## [x] Core Engine

- [x] Implement the Log-Structured Merge-Tree (LSMT) for Context Block storage.
- [x] Finalize on-disk format for blocks and graph edges.
- [x] Implement the Virtual File System (VFS) interface.
- [x] Implement Context Block data structure with proper serialization.
- [x] Basic Query Engine: Retrieve blocks by ID.
- [x] Write-Ahead Log (WAL) implementation for durability.
- [x] In-memory block index with hash-based lookups.
- [x] Graph edge storage and serialization.
- [x] Block validation and error handling.
- [x] Working demo with LLM-formatted output.

## [ ] Replication & Durability

- [ ] Implement the replication protocol (Viewstamped Replication or similar).
- [ ] Add checksumming for all on-disk data structures.
- [ ] Implement cluster membership and leader election.

## [ ] Ingestion Pipeline

- [ ] Build the framework for `Source Connectors`.
- [ ] Implement initial `Source Connector`: Git repositories.
- [ ] Implement initial `Parser`: Extract functions/structs from Zig source code.
- [ ] Implement `Chunker` to intelligently split large documents.

## [ ] Query Engine V2

- [ ] Implement graph traversal queries.
- [ ] Add filtering capabilities based on metadata.
- [ ] PoC for optional semantic search via an external embedding model.

## [ ] Client & Tooling

- [ ] Finalize the binary client-server protocol.
- [ ] Implement the Zig client library.
- [ ] Expand the CLI with more management commands (e.g., `status`, `query`, `backup`).

## [x] Simulation & Testing

- [x] Create simulation scripts for common failure modes (network partitions, disk faults).
- [x] Implement comprehensive assertion framework for defensive programming.
- [x] Build deterministic simulation harness with seeded PRNG.
- [x] Implement deterministic VFS for simulation testing.
- [x] Create example simulation test cases with network partition scenarios.
- [x] Fix pre-commit check failures and implement proper function naming enforcement.
- [x] Debug simulation test segfault (directory creation issue).
- [x] Comprehensive storage engine stress testing (high volume writes, large blocks, rapid updates).
- [x] WAL corruption and recovery simulation scenarios.
- [x] Network partition tolerance testing during heavy load.
- [x] Invalid data handling and robustness testing.
- [x] Memory leak detection and resource management validation.
- [x] Deterministic replay testing with identical seeds.
- [ ] Add fuzz testing for the ingestion parser.
- [ ] Benchmark performance under simulated load.

## [ ] Next Priority Items

- [ ] Implement WAL recovery on storage engine startup.
- [ ] Add SSTable generation and compaction for LSM-Tree.
- [ ] Implement graph traversal queries (BFS/DFS).
- [ ] Add secondary indexes for metadata filtering.
- [ ] Implement basic replication between nodes.
- [ ] Build the ingestion pipeline framework.
