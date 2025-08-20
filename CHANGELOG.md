# Changelog

## [0.1.0] - 2025-08-19

Initial release. Core storage and query engine.

### Features

- LSM-Tree storage engine with WAL durability
- Graph traversal with typed edges
- Arena coordinator memory pattern
- Deterministic simulation testing
- Binary protocol server

### Performance

- 30µs block writes (33K ops/sec) optimized benchmark
- 33ns block reads (29.9M ops/sec) from memtable
- 56ns single queries (17.9M ops/sec)
- O(1) memory cleanup through arena coordinator

### Known Limitations

- Single-node only
- Basic query operations only
- No authentication

### Dependencies

- Zig 0.15.1
- Zero runtime dependencies
