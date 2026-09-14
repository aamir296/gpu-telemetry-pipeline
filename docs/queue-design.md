# Custom queue design

## Wire protocol

Every TCP request/response has a fixed 24-byte network-byte-order header:

| Field | Bytes | Purpose |
|---|---:|---|
| Magic | 4 | `GTQ1` protocol identification |
| Version | 1 | compatibility fencing |
| Operation | 1 | acquire, publish, fetch, commit, and membership operations |
| Flags | 2 | reserved evolution space |
| Request ID | 8 | response correlation |
| Payload length | 4 | allocation boundary |
| CRC32C | 4 | corruption detection |

Membership/cycle commands use JSON because they are low volume and operable. Telemetry batches and deliveries use a versioned length-prefixed binary codec. Both frame and record decoding enforce size/count limits before allocation.

## Durable log

GPU key hashing selects one of a fixed number of partitions. Each partition owns monotonically increasing offsets and ordered segment files. Disk records contain payload length, offset, CRC32C, and the binary event. Batches take a partition lock and receive a single `fsync` before success is returned.

Startup first validates persisted format metadata and the immutable partition count, then scans segments in order and reconstructs offset and source-key indexes. An incomplete tail is safely truncated. Invalid lengths, invalid payloads, gaps used by a fetch, or checksum failure stop the broker; they are treated as corruption requiring operator investigation.

Consumer commits are monotonic next-offset maps stored through write-temp-and-rename. Fetch starts from the durable group offset. A fetch reuses one file descriptor per segment in the batch, and reclamation deletes the source-key index in O(1) per removed record. The collector never advances an offset before PostgreSQL commits.

## Ownership

Producer membership is scoped by dataset. Consumer membership is scoped by group. Adding or expiring a member changes a generation. A streamer renews its lease by reacquiring the current assignment at a five-second default interval while reading a cycle; it aborts if the returned cycle, generation, or member set changed. The caller's renewal request itself proves it is alive, so a long valid cycle cannot expire and re-add that same producer merely because the CSV takes longer than the lease to emit. Rendezvous hashing maps GPU keys to producers and queue partitions to consumers while minimizing movement. Every publish includes dataset, cycle, generation, and producer; the broker rejects a stale or wrong owner.

The broker is the cycle authority. Cycle `N+1` cannot begin until all currently active owners complete `N` and the interval floor passes. Cycle state is persisted. After restart, the same cycle is replayed from row one under rebuilt membership; durable source-key dedup prevents duplicates.

## Explicit limits

The broker enforces frame, record, batch, partition, segment, and total log byte limits. A closed segment is reclaimed only after every known consumer group has committed beyond all of its offsets; the active segment and all unconsumed data are retained. Total capacity is a safety boundary, not silent data eviction: publication backpressures if consumers cannot free space. Long-term archival and replicated high availability are intentionally outside this single-node exercise implementation and are called out in the README instead of being implied.
