# Data Model Specification

This document defines the byte-level layout of the primary data structures in CortexDB. All multi-byte integers are little-endian. All strings are UTF-8 encoded and are not null-terminated; their length is specified separately.

## On-Disk Structures

These structures define how data is laid out in the LSM-Tree's SSTable files and in the Write-Ahead Log (WAL).

### `LSMTLogEntry`

The atomic unit of the WAL. Represents a single operation in a transaction.

| Offset | Size (bytes) | Field          | Description                                             |
|--------|--------------|----------------|---------------------------------------------------------|
| 0      | 8            | `checksum`     | CRC-64 checksum of the `type` and `payload`.            |
| 8      | 1            | `type`         | `0x01` = PutBlock, `0x02` = DeleteBlock, `0x03` = PutEdge |
| 9      | 4            | `payload_size` | Length of the payload in bytes (little-endian).         |
| 13     | `payload_size` | `payload`    | The data for the operation (e.g., a serialized `ContextBlock`). |

**Total header size:** 13 bytes (8 + 1 + 4)

### `ContextBlock`

The core unit of knowledge. This is a variable-length structure.

| Offset               | Size (bytes)     | Field                | Description                                         |
|----------------------|------------------|----------------------|-----------------------------------------------------|
| 0                    | 16               | `id`                 | 128-bit ULID. Time-ordered and unique.              |
| 16                   | 8                | `version`            | Monotonically increasing version number for this block. |
| 24                   | 4                | `source_uri_len`     | Length of the source URI string.                    |
| 28                   | 4                | `metadata_json_len`  | Length of the metadata JSON string.                 |
| 32                   | 4                | `content_len`        | Length of the content payload.                      |
| 36                   | `source_uri_len` | `source_uri`         | The URI of the block's origin (e.g., git://...).   |
| `36 + uri_len`       | `metadata_len`   | `metadata_json`      | A JSON string of key-value metadata.                |
| `36 + uri+meta_len`  | `content_len`    | `content`            | The raw content of the block.                       |

*Diagram of a serialized block:*
`| id (16) | ver (8) | uri_len (4) | meta_len (4) | content_len (4) | uri (...) | metadata (...) | content (...) |`

### `GraphEdge`

Represents a directed relationship between two Context Blocks.

| Offset | Size (bytes) | Field        | Description                                       |
|--------|--------------|--------------|---------------------------------------------------|
| 0      | 16           | `source_id`  | The 128-bit ID of the block where the edge starts. |
| 16     | 16           | `target_id`  | The 128-bit ID of the block where the edge ends.  |
| 32     | 2            | `type_id`    | An enum representing the relationship (`DEFINED_IN`, etc.). |
| 34     | 6            | `reserved`   | Padding for future use. Must be zero.             |


## Storage Layout

The `data` directory has the following structure:

-   `/wal/`: Directory for Write-Ahead Log files. Files are named `wal_XXXX.log`.
-   `/sst/`: Directory for SSTable files, which contain the sorted, immutable data.
-   `MANIFEST`: A file that describes the current state of the LSM-Tree, pointing to the active SSTable files.
-   `LOCK`: A file lock to prevent multiple instances from using the same data directory.
