# Production Readiness

## Current status

This repository is an experimental distributed key-value store and is **not yet ready for production data**. It has a runnable C++/gRPC Raft cluster, persistent log and metadata, snapshots, a browser gateway, optional mutual TLS, and automated tests. Those features are a starting point, not evidence of production-grade safety, availability, or support.

## Starting the production-readiness work

### Phase 1: Define a supported deployment profile (current first step)

Before any trial deployment, the project needs an explicit, conservative operating envelope. The current implementation is a static-cluster Raft system designed for local/demo deployment, not a general-purpose production service. The initial release target should therefore be narrow and well-documented.

- Supported cluster sizes: start with a static odd-sized cluster of 3 or 5 nodes only; do not advertise support for larger or elastic membership without a validated consensus-safe membership protocol.
- Deployment topology: single datacenter or tightly controlled private network only; no cross-region or public internet exposure.
- Workloads: small-to-medium key/value workloads with measured latency and throughput; no unbounded bulk ingestion or large-object writes until capacity tests are complete.
- Data model: start with small values and predictable access patterns; define maximum key size, value size, and write rates before rollout.
- Consistency: treat the cluster as a majority-acknowledged replicated log system with Raft semantics; document the exact failure model that is guaranteed and the cases that are not covered.
- Availability target: define a target for leader failover time, restart recovery time, and acceptable outage windows for the initial deployment profile.
- Recovery objectives: document recovery time and data-loss expectations for process crashes, node restarts, and disk corruption scenarios; default to conservative assumptions until validated.
- OS and storage: document the supported Linux distributions, filesystem assumptions, local disk expectations, and any limits around WAL/snapshot durability.
- Network assumptions: name the required private-network assumptions, port access rules, and latency/jitter expectations for inter-node communication.
- Upgrade policy: define supported rolling upgrades, restart procedures, and rollback constraints; the static peer list must remain consistent across the upgrade path.
- Release scope: initially treat this as a controlled internal deployment product, not a general public service, unless the security, recovery, and operational controls are explicitly completed and validated.

This is the minimum scope needed before the project can claim any production-like readiness. It should be signed off before moving into broader deployment testing.

### Phase 2: Raft safety and recovery review (next)

The implementation must be checked against the Raft safety rules and failure model before any additional rollout assumptions are made.

- Verify term changes, election fairness, voting rules, log matching, commit advancement, leader changes, and snapshot installation.
- Confirm that peer reloads are treated as a config convenience only and not as a safe membership-change mechanism.
- Test partitions, delayed or reordered messages, repeated crashes, disk-full and I/O errors, and stale or lagging nodes.
- Validate persistence and recovery after truncated metadata or snapshots, and confirm crash recovery behavior matches the documented guarantees.
- Require long-running multi-node soak tests and an independent review of the consensus and persistence path.

### Phase 3: Security hardening baseline (required before deployment)

The system must treat security as a deployment prerequisite, not a later add-on. Dashboard authentication alone is not sufficient protection for the cluster.

- Authenticate and authorize direct gRPC clients using an explicit identity model, not just browser session cookies.
- Enforce least privilege across reads, writes, admin actions, backups, and restores; no service accounts should be over-privileged.
- Set deployment-safe defaults for TLS, bind addresses, listening ports, gateway origins, session secrets, and network exposure.
- Require mTLS or equivalent authenticated transport for inter-node and client traffic in any environment beyond controlled internal testing.
- Document certificate issuance, secret provisioning, rotation, revocation, expiry handling, and emergency key replacement.
- Define network trust boundaries and ensure public exposure never includes unauthenticated gRPC or demo gateway mode.
- Review dependencies, configuration handling, and threat model; test abuse cases, resource exhaustion, auth failures, and connection limits.
- Require a documented approval process for production secrets and deployment configuration before cluster startup.

### Completion criteria for Phases 1-3

The project may move past the early readiness stage only when all of these conditions are met:

- The deployment profile is written down and approved for a limited, controlled environment.
- The Raft safety review has been completed with explicit failure-mode evidence and soak-test results.
- The security baseline is documented and implemented for direct client access, TLS, least privilege, and secret handling.
- All three sections are treated as release gates rather than optional future work.

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