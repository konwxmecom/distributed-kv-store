# Operations runbook

This runbook describes cautious backup, restore, and recovery procedures for the
current experimental implementation. It is not a production support guarantee.
No restore drill or disaster-recovery objective is implied by the presence of
these instructions; validate them against a disposable cluster before relying
on them.

## Health and readiness probes

The gateway's `/health` endpoint checks whether its configured gRPC target
responds; it is a transport-liveness check, not evidence that the cluster can
commit writes. The `/ready` endpoint returns success only when that target
reports itself as a ready leader, and returns `503` for followers or RPC errors.
These distinct behaviors are covered by gateway tests.

Use `/ready` only when routing traffic specifically to the active leader.
Follower nodes may still forward client operations, but the endpoint deliberately
does not count them as leader-ready. Neither probe defines alert thresholds,
restart policy, quorum recovery, or an availability objective; operators must
establish those from staging observations.

## Backup choices

### Dashboard account export

The dashboard backup endpoint exports one account as JSON. Sign in as that
account, download its backup, and store it outside the cluster host. Keep
separate exports for each account.

This export is not an atomic point-in-time snapshot: it lists keys and reads
their values in separate RPCs. Stop writes to the account for the duration of
the export if a consistent logical backup is required. The restore endpoint
merges listed keys and does not delete keys absent from the backup; it accepts
at most 10,000 entries. Treat the JSON as sensitive application data and apply
the same access controls and retention policy as the stored values.

### Node data directories

Each node's `raft.meta`, `raft.snapshot`, and `raft.wal` are a coordinated
storage set; do not copy only one file or copy a live data directory. To make a
filesystem copy:

1. Quiesce application writes and stop the gateway and all clients.
2. Stop every node cleanly (the project launcher can be stopped with
   `./stop.sh`) and confirm all node processes have exited.
3. Copy the complete runtime data root, preserving every `node_<port>` directory
   and file, to protected storage on a different host or failure domain.
4. Record the application version, node ports/static peer list, data-root path,
   backup time, and a checksum manifest alongside the copy.
5. Verify that the copied files are readable and that the off-host copy is
   included in the chosen retention policy.

Do not treat the launcher's `/tmp` defaults as durable backup storage. Select
and approve backup frequency, retention, encryption, access control, and
off-host failure domain for the intended deployment; this repository does not
set those policies.

## Restore drill

Perform drills only with disposable data and an isolated host or environment.

1. Select a backup and record its source application version and static cluster
   configuration.
2. Create a new, empty data root. Never merge an old backup into a live data
   root or reuse the production data root for a drill.
3. For a filesystem restore, copy the entire saved data root into the empty
   location, then start all nodes with their original ports, peer lists, TLS
   configuration, and the restored root. For a dashboard export, start an
   isolated cluster and use the dashboard restore control while signed in as
   the matching account.
4. Verify node health, leader/quorum behavior, expected keys and values, and
   application-level reads and writes. Check that keys omitted from a dashboard
   export remain present, since restore merges rather than replaces.
5. Record elapsed recovery time, missing or unexpected data, errors, and the
   exact versions/configuration. Do not call the drill successful until the
   expected data has been checked.

The dashboard export/import path is not a substitute for a full-cluster
disaster-recovery test. A filesystem copy, recovery after loss of a quorum, and
replacement of a failed node have not been independently validated here.

## Disaster recovery and node replacement

Keep backups off-host and ensure operators can retrieve them when the cluster
host is unavailable. Define and approve RTO and RPO from measured drills before
setting operational targets; none are established by this project.

Voting membership is static. Do not change the peer list as a node-replacement
shortcut or restore one node's files into a different node identity. Test
replacement only in an isolated environment with the exact intended ports,
peer list, TLS identities, and data-root layout. Preserve the original backup
until the replacement has caught up and application data has been verified.
If a quorum is lost or recovery behavior is unclear, stop writes and preserve
all node directories for independent recovery review instead of experimenting
on the only copy.

## Upgrade and rollback

There is no supported rolling-upgrade or general on-disk rollback procedure.
Snapshot and WAL readers retain compatibility with earlier local-development
formats, but that is not a versioned compatibility promise. Before changing
the application version, quiesce writes, make and verify a complete
off-host backup, and test the exact upgrade and rollback path on a disposable
copy. Do not run an older binary against data modified by a newer binary unless
that exact rollback has been tested and approved.

## Operational records

For each backup and restore drill, record:

- timestamp, application version, static node/peer configuration, and data root
- backup type, location, checksum result, and retention expiry
- operator and verification outcome
- restore duration, observed data loss, and follow-up actions

Keep credentials and private keys out of these records. Follow the deployment
owner's approved secret storage and incident-handling procedures.
