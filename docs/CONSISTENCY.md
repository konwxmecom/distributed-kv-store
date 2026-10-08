# Consistency and durability

This document describes the behavior implemented by the current demo server. It
is not a production guarantee; see [../PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)
for failure modes that still need independent validation.

## Writes

`Set` and `Delete` are accepted only by the ready leader (followers forward to a
known leader). A successful response means the entry was appended to the local
WAL, acknowledged by a majority of the configured static membership, and applied
to the state machine. The leader only advances `commit_index` using a replicated
entry from its current term. A newly elected leader appends a no-op entry before
reporting itself ready; committing that entry also commits any preceding
majority-replicated entries, as required by Raft.

WAL and metadata writes are flushed and synchronized before acknowledgement. This
depends on the operating system, filesystem, storage device, and their honoring
`fsync`; it does not protect against arbitrary hardware that lies about durable
writes, loss of a majority, or operator deletion/corruption of data.

Providing a non-empty `request_id` makes a retry of the same request return its
original result, including after restart and snapshot recovery. The caller must
reuse the ID for retries. IDs are opaque and are not scoped to authenticated
clients; callers must make them unique. Requests without an ID are not
deduplicated. This is an exactly-once *effect for a reused ID*, not exactly-once
network delivery.

## Reads

The default `STRONG` consistency mode is served only after the ready leader
confirms a majority of same-term peers and rechecks its term/role. A follower
forwards strong reads to that leader and returns `UNAVAILABLE` if it cannot
confirm a leader. This is a quorum-confirmed leader read, not a formal ReadIndex
implementation; do not assume guarantees stronger than the implemented
same-term quorum round trip without model checking and fault-injection evidence.

`EVENTUAL` reads may be served from a follower's locally applied state. They can
be stale by an unbounded amount during a partition or while a node catches up.

## Membership and security boundaries

The voting membership is static. Reloading a peer file changes outbound
connections only; it is not a consensus-safe membership change. Direct gRPC
clients are not authenticated unless mTLS is configured. Gateway login does not
secure direct gRPC access, and insecure gRPC must remain on a trusted local
network.

## Storage compatibility

Snapshots include a format marker and CRC-protected protobuf payload. The reader
continues to accept the earlier unversioned snapshot and WAL formats for local
development. Keep a backup before upgrading data files; there is no supported
rolling on-disk or wire-format upgrade procedure yet.
