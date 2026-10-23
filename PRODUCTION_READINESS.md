# Production Readiness

## Current status

This repository is an experimental distributed key-value store and is **not yet ready for production data**. It has a runnable C++/gRPC Raft cluster, persistent log and metadata, snapshots, a browser gateway, optional mutual TLS, and automated tests. Those features are a starting point, not evidence of production-grade safety, availability, or support.

## Progress tracker

✅ means the specific documentation or code change is present. It does **not** mean the production capability has been independently validated. ⬜ means work remains.

### Completed in this repository

- ✅ Drafted an initial, conservative deployment profile: static 3- or 5-node clusters, private single-datacenter network, and controlled internal-use scope. This profile is not yet measured, approved, or a production support guarantee.
- ✅ Documented that reloading the peer file is not a safe Raft voting-membership change; static membership remains the only supported mode.
- ✅ Gateway now defaults to loopback binding. Public binding requires explicit opt-in and a configured users file; regression tests cover these checks.
- ✅ Dashboard account authentication, per-user key isolation, and password-file permissions are implemented and covered by gateway tests. This does not authenticate direct gRPC clients.
- ✅ mTLS options are implemented for server/client/gateway configuration, but mTLS is optional and is not yet enforced as a deployment default.

### Phase 1: Supported deployment profile — incomplete

The initial profile above is a draft, not a finalized contract. Complete and approve:

- ⬜ Measure and set supported workload, key/value limits, and write-rate limits.
- ⬜ Define consistency guarantees, availability targets, and recovery objectives (RTO/RPO) from verified behavior.
- ⬜ Specify supported OS versions, filesystems/storage assumptions, network requirements, and upgrade/rollback policy.
- ⬜ Approve whether the supported release scope is controlled internal use or general public use.

### Phase 2: Raft safety and recovery — incomplete

- ⬜ Audit term changes, elections/voting, log matching, commit advancement, leader transitions, and snapshot installation against Raft safety rules.
- ✅ Keep voting membership static and clearly identify peer-file reload as endpoint configuration only; safe membership changes are not implemented.
- ✅ Test recovery of valid committed data after a truncated trailing WAL header.
- ✅ Test that a successfully acknowledged write survives an abrupt single-node process kill and restart.
- ⬜ Test partitions, delayed/reordered messages, repeated crashes, disk-full/I/O errors, corrupt or truncated metadata/snapshots, and stale/lagging node recovery.
- ⬜ Verify acknowledged-write durability for the explicitly supported crash and power-loss model beyond the tested single-process-kill case.
- ⬜ Add long-running multi-node soak tests and obtain independent consensus/persistence review.

### Phase 3: Security — incomplete

- ⬜ Authenticate and authorize direct gRPC clients; dashboard login does not protect direct gRPC access.
- ⬜ Implement and verify least-privilege access for reads, writes, administration, backups, and restores.
- ⬜ Require TLS/mTLS by default for supported deployments and define safe node bind/network defaults.
- ⬜ Document and validate certificate/secret provisioning, rotation, revocation, expiry, and emergency replacement.
- ✅ Make gateway loopback binding the default; require explicit opt-in and configured dashboard authentication for non-loopback/public binding.
- ⬜ Complete dependency/configuration/threat-model review and test abuse, resource exhaustion, authentication failures, and connection limits.
- ⬜ Establish approval and handling procedures for production credentials and configuration.

### Phase 4: Operations and observability — incomplete

- ⬜ Add structured, configurable logs and production metrics for Raft state, replication lag, WAL/snapshot activity, request latency/errors, and resource use.
- ✅ Expose standard gRPC process health and mark the node not-serving before its existing bounded graceful shutdown. This does not indicate Raft quorum or application-level readiness.
- ⬜ Define alerts, Raft-aware readiness behavior, restart policy, and capacity guidance.
- ✅ Document cautious backup, off-host storage, restore-drill, disaster-recovery, and upgrade/rollback procedures in [docs/OPERATIONS.md](docs/OPERATIONS.md). Procedures still require disposable-environment drills and approved retention/RTO/RPO policies.
- ⬜ Provide reproducible packaging/deployment, configuration examples, and an upgrade/rollback procedure.

### Phase 5: Performance and release validation — not started

- ⬜ Run representative load, latency-percentile, capacity, and resource-consumption tests across supported cluster sizes.
- ⬜ Run repeatable fault-injection and recovery tests in CI or a dedicated staging environment.
- ⬜ Establish release gates for clean builds, C++/Python tests, sanitizers, security checks, restore drills, and staging soak tests.
- ⬜ Publish known limitations, compatibility policy, and a versioned release process. The initial operations runbook is [docs/OPERATIONS.md](docs/OPERATIONS.md); publication alone is not operational validation.

## Before any trial deployment

- ⬜ Use only disposable or fully backed-up data until recovery behavior has been independently validated.
- ⬜ Restrict node and gateway ports to a trusted private network; do not expose unauthenticated gRPC or demo gateway mode publicly.
- ✅ Keep cluster voting membership static; do not treat peer-file reload as a live membership change.
- ⬜ Test backup restoration and node replacement using the exact deployment configuration.