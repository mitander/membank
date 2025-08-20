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

- 15µs block writes (67K ops/sec)
- 0.08µs block reads (12.6M ops/sec)
- Sub-100µs graph traversal

### Known Limitations

- Single-node only
- Basic query operations only
- No authentication

### Dependencies

- Zig 0.15.1
- Zero runtime dependencies
