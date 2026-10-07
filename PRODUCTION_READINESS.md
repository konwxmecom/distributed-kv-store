# Production Readiness

## Current status

This repository is an experimental distributed key-value store and is **not yet ready for production data**. It has a runnable C++/gRPC Raft cluster, persistent log and metadata, snapshots, a browser gateway, optional mutual TLS, and automated tests. Those features are a starting point, not evidence of production-grade safety, availability, or support.

## Work remaining

### Requirements and supported deployment

- Define supported cluster sizes, workloads, key/value limits, consistency guarantees, availability targets, and recovery objectives.
- Document the supported operating systems, storage/filesystem assumptions, network requirements, and upgrade policy.
- Decide whether the product is intended for a controlled internal deployment or general public use; scope the release requirements accordingly.

### Raft safety and recovery

- Review the implementation against the Raft safety rules, including term changes, voting, log matching, commit advancement, leader changes, and snapshot installation.
- Add and validate safe membership changes (joint consensus or an equivalent proven protocol). Editing the peer file currently reloads outbound endpoints; it is **not** a safe voting-membership change.
- Test partitions, delayed/reordered messages, repeated crashes, disk-full and I/O errors, corrupt/truncated metadata and snapshots, and recovery with stale or lagging nodes.
- Verify that every acknowledged write survives the documented failure model, including process crashes and host/power loss where supported.
- Add long-running multi-node soak tests and independent review of the consensus and persistence code.

### Security

- Authenticate and authorize direct gRPC clients; dashboard login only protects gateway/dashboard accounts.
- Define and enforce least-privilege access for reads, writes, administration, backups, and restores.
- Set deployment-safe defaults for TLS, bind addresses, gateway origins, session secrets, and network exposure.
- Document certificate and secret provisioning, rotation, revocation, and expiry handling.
- Perform dependency, configuration, and threat-model reviews; test abuse cases and resource limits.

### Operations and observability

- Add structured, configurable logs and production-grade metrics for Raft state, replication lag, disk/WAL/snapshot activity, request latency/errors, and resource use.
- Define alerts, health/readiness behavior, graceful shutdown, restart policy, and capacity guidance.
- Document backup retention, off-host storage, restore drills, disaster recovery, and data migration procedures.
- Provide reproducible packaging/deployment (for example, container images or signed release artifacts), configuration examples, and an upgrade/rollback procedure.

### Performance and release validation

- Run representative load, latency-percentile, capacity, and resource-consumption tests across supported cluster sizes.
- Run repeatable fault-injection and recovery tests in CI or a dedicated staging environment.
- Define release gates: clean build, all C++ and Python tests, sanitizer results, security checks, restore drill, and staging soak test.
- Publish known limitations, compatibility policy, operational runbooks, and a versioned release process.

## Before any trial deployment

- Use only disposable or fully backed-up data until recovery behavior has been independently validated.
- Restrict node and gateway ports to a trusted private network; do not expose unauthenticated gRPC or demo gateway mode publicly.
- Keep cluster voting membership static; do not treat peer-file reload as live membership change.
- Test backup restoration and node replacement using the exact deployment configuration.