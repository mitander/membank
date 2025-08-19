# Storage Architecture

LSM-Tree optimized for write throughput with deterministic recovery.

## Components

**WAL**: 64MB segments, CRC-64 checksums, sequential writes only
**Memtable**: In-memory HashMap, arena-allocated
**SSTables**: Immutable sorted files, Bloom filters, 64-byte aligned
**Compaction**: Size-tiered, background thread, no write blocking

## Write Path

```
Client → WAL append → Memtable insert → Response
                          ↓ (threshold)
                     SSTable flush
```

## Read Path

```
Memtable → SSTable index → Disk read
   ↓ miss      ↓ bloom filter
  next       skip if no match
```

## Corruption Detection

`CorruptionTracker` monitors WAL failures. 4+ consecutive checksum failures = fail-fast.

```zig
if (tracker.consecutive_failures >= 4) {
    fatal_assert(false, "Systematic WAL corruption", .{});
}
```

## On-Disk Format

```
SSTable Header (64 bytes):
[8: magic][8: version][8: block_count][8: min_id][8: max_id][24: reserved]

Block Entry:
[8: block_id][4: offset][4: size][8: checksum]
```

## Performance

- Sequential writes: 500MB/s
- Random reads: 0.08µs with cache hit
- Compaction: 100MB/s merge rate
- Recovery: <1s per GB
