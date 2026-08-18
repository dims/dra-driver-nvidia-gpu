---
title: Persistent ComputeDomain agents
authors:
- NVIDIA
reviewers:
- TBD
approvers:
- TBD
creation-date: 2026-08-16
status: implementable
---

# Persistent ComputeDomain agents

## Executive decision

Add one alpha installation mode, `PersistentComputeDomainAgents`, which replaces the historical per-ComputeDomain IMEX daemon with one reusable agent Pod on each capable Node.

This is a switch for the whole driver installation. It is not a second per-ComputeDomain protocol, and it is not supported beside the old daemon on the same Node. With the gate off, behavior stays on the existing legacy path. Before turning the gate on, operators must retire all ComputeDomains and their daemon claims. Once enabled, every new ComputeDomain uses the persistent fleet.

Today, each ComputeDomain starts a new daemon Pod on every selected Node. At large node counts, workload startup includes scheduler, ResourceClaim, DaemonSet, Pod, image, and process convergence for an entire extra fleet. The new mode moves that machine-local setup ahead of the workload. A small agent is already present on each eligible Node and starts the IMEX child only after the controller publishes an authorized peer map.

The change removes the per-ComputeDomain daemon DaemonSet, daemon ResourceClaimTemplate, daemon ResourceClaims, and daemon Pods from the startup path. It does not remove the workload Pod, workload channel claim, ComputeDomain, or the safety state needed to allocate stable clique indexes and prove shutdown. The controller still derives membership from allocated workload claims, and the kubelet still refuses a workload until the local agent has installed the exact published generation.

The agent is a DaemonSet, but its role is different from the DaemonSet used today. The old DaemonSet is created after each ComputeDomain and is itself part of that ComputeDomain's convergence. The new DaemonSet is created once when the driver installation is prepared. Workload startup reuses its already-scheduled Pods, already-allocated daemon devices, and already-running control loop.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 270" role="img" aria-labelledby="startup-title startup-desc">
  <title id="startup-title">Startup before and after persistent agents</title>
  <desc id="startup-desc">The legacy path creates a daemon fleet per ComputeDomain. The persistent path reuses an installation fleet and creates only authorization state.</desc>
  <style>
    .box { fill: #f7f7f7; stroke: #444; stroke-width: 1.5; rx: 8; }
    .accent { fill: #e8f5e9; stroke: #2e7d32; stroke-width: 1.5; rx: 8; }
    .text { font: 15px sans-serif; fill: #222; }
    .small { font: 13px sans-serif; fill: #333; }
    .arrow { stroke: #555; stroke-width: 2; marker-end: url(#arrow); }
  </style>
  <defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3 z" fill="#555"/></marker></defs>
  <text class="text" x="20" y="28">Legacy installation</text>
  <rect class="box" x="20" y="48" width="165" height="58"/><text class="small" x="41" y="81">ComputeDomain</text>
  <line class="arrow" x1="185" y1="77" x2="225" y2="77"/>
  <rect class="box" x="225" y="48" width="210" height="58"/><text class="small" x="247" y="73">Create daemon claims,</text><text class="small" x="247" y="91">Pods, and processes</text>
  <line class="arrow" x1="435" y1="77" x2="475" y2="77"/>
  <rect class="box" x="475" y="48" width="190" height="58"/><text class="small" x="499" y="81">Publish membership</text>
  <line class="arrow" x1="665" y1="77" x2="705" y2="77"/>
  <rect class="box" x="705" y="48" width="225" height="58"/><text class="small" x="733" y="81">Release workload channel</text>

  <text class="text" x="20" y="158">Persistent-agent installation</text>
  <rect class="accent" x="20" y="178" width="165" height="58"/><text class="small" x="42" y="202">Agent already</text><text class="small" x="42" y="220">running per Node</text>
  <line class="arrow" x1="185" y1="207" x2="225" y2="207"/>
  <rect class="accent" x="225" y="178" width="210" height="58"/><text class="small" x="247" y="202">Authorize exact clique</text><text class="small" x="247" y="220">and peer generation</text>
  <line class="arrow" x1="435" y1="207" x2="475" y2="207"/>
  <rect class="accent" x="475" y="178" width="190" height="58"/><text class="small" x="497" y="202">Agent starts child and</text><text class="small" x="497" y="220">writes exact receipt</text>
  <line class="arrow" x1="665" y1="207" x2="705" y2="207"/>
  <rect class="accent" x="705" y="178" width="225" height="58"/><text class="small" x="733" y="211">Release workload channel</text>
</svg>

## Goals

- Remove per-ComputeDomain daemon scheduling and claim allocation from workload startup.
- Preserve deterministic clique indexes and fail-closed membership.
- Keep default, gate-off behavior compatible with the main branch.
- Continue reconciling and retiring existing persistent state after the gate is disabled.
- Bound normal formation to one reservation, one snapshot create, one finalizer update, one status publication, and one Node attestation update per member.
- Support multiple physical cliques in one ComputeDomain.
- Make every safety-sensitive identity durable and inspectable.

## Non-goals

- Running legacy per-domain daemons and persistent agents on the same Node.
- Selecting the provider with a user annotation or per-ComputeDomain canary.
- In-place conversion of an active legacy ComputeDomain.
- Automatic recovery from an ambiguous same-boot agent Pod replacement.
- Supporting this alpha on OpenShift or on Kubernetes versions without `resource.k8s.io/v1`.
- Removing the need for a verified whole-clique reset when prior runtime state is uncertain.

## Installation contract

The feature gate has exactly two meanings:

| Gate | Provider for new ComputeDomains |
|---|---|
| `PersistentComputeDomainAgents=false` | Existing per-ComputeDomain daemon |
| `PersistentComputeDomainAgents=true` | Installation persistent-agent DaemonSet |

The controller persists the chosen provider as `resource.nvidia.com/computeDomainCliqueProtocol`. Users cannot create, change, or remove this marker. Marker-less ComputeDomains which predate this feature remain legacy.

Enabling the gate requires:

- driver-managed IMEX;
- Kubernetes v1.34 or newer with `resource.k8s.io/v1`;
- `ComputeDomainCliques`, `IMEXDaemonsWithDNSNames`, and `CrashOnNVLinkFabricErrors`;
- kubelet GPU-clique label publication;
- controller leader election;
- the persistent-agent admission boundary;
- one admin-controlled installation and driver namespace;
- zero legacy ComputeDomains and zero per-ComputeDomain daemon artifacts.

The default installation does not start persistent informers or agents.

## Resources

The mode adds three CRDs:

- `ComputeDomainCliqueReservation` is the cluster-scoped, API-server-atomic owner of one physical clique.
- `ComputeDomainCliqueSnapshot` is the namespaced desired membership and stable index map for one ComputeDomain and clique.
- `ComputeDomainCliqueRetirementEvidence` is an immutable statement from an exact agent identity that its child stopped or the Node rebooted.

It also adds one installation ResourceClaimTemplate and one `OnDelete` DaemonSet. The DaemonSet requests the same exclusive daemon device used by the legacy path. That exclusivity is why migration must drain the old provider before creating the fleet.

## Formation

1. A workload Pod is scheduled with the canonical generated channel claim.
2. The controller validates the live Pod, generated ResourceClaim, ResourceClaimTemplate, allocation, `reservedFor` Pod UID, selected Node, ComputeDomain UID, and persisted provider.
3. Every physical-clique Node must carry the immutable startup clique identity and the operator's whole-clique isolation label for this ComputeDomain. A bare or foreign legacy route blocks formation.
4. The controller atomically creates or validates the physical-clique reservation.
5. The controller writes the claim-backed Node route and attestation.
6. Once the declared `spec.numNodes` expected set is complete, the controller assigns stable indexes and publishes one Active snapshot per clique.
7. Each local agent validates its exact Node UID, Pod UID, Pod IP, reservation, snapshot, and index, starts the IMEX child, waits for READY, and writes an exact applied receipt.
8. The kubelet releases the workload channel only when its cached snapshot, current agent Pod, reservation, and node-local receipt all agree.

No partial expected set becomes Active. An ambiguous member departure freezes the last authorized map.

## Retirement and reuse

Deletion is a protocol, not garbage collection:

1. The controller changes the snapshot to `Retiring` and freezes the last published identity.
2. Each agent stops its matching child.
3. The same published Pod may attest a normal process exit. A replacement Pod may attest only after a Node reboot supplies a different boot ID.
4. The controller verifies exact evidence for every published member and changes the snapshot to `Fenced`.
5. It marks the reservation `Released`, removes routing metadata, deletes evidence and snapshot state, and removes the ComputeDomain finalizer.
6. The released clique ID may then be reserved by another ComputeDomain.

A same-boot replacement agent is intentionally not trusted as proof that the old process stopped. It is quarantined. Because one agent can serve many sequential domains, operators must not delete an Active agent Pod as a routine rollout. Retire its active ComputeDomain first. The `OnDelete` strategy makes that operational choice explicit.

Disabling the feature gate affects only provider selection for new ComputeDomains. The controller and kubelet discover persisted markers and reservations and keep the persistent readers and retirement state machine running. The retained DaemonSet and admission policies must remain until all persistent state is released.

## Whole-clique isolation

The reservation is keyed by physical clique, not by Node or namespace. The Node isolation label creates a boundary before claim-backed routing is published. Admission allows the transition only while the old and new route and attestation are empty, closing the race with legacy membership writes at the Node resource version.

This does not prove that an unrecorded historical IMEX process is dead. The first canary on a clique therefore requires new hardware or an external quiesce/reset. The alpha does not automate that proof.

## Failure behavior

| Failure | Behavior |
|---|---|
| Expected Node or claim missing | Pending; no Active snapshot |
| Extra or unattested Node | Pending and observable condition/Event |
| Agent not Ready | Snapshot may exist; workload remains blocked |
| Snapshot corruption | Agent and kubelet reject it |
| Member label/topology disappears | Freeze last Active map |
| Same-boot agent replacement | Quarantine; no retirement claim |
| Verified Node reboot | Replacement may attest `NodeReboot` |
| API timeout or conflict | Retry; do not convert to permanent workload failure |
| Gate disabled with persistent state | Continue reconciliation and retirement |
| Leader lost | Stop and join workers before successor writes |
| Missing APIs or admission guard | Refuse persistent operation |

## Scaling model

Persistent agents remove the largest startup fan-out: per-domain daemon claims and Pods. The clique protocol's first-formation work for `N` selected Nodes across `C` physical cliques is exactly `N + 5*C` confirmed writes in the healthy path: `N` Node attestations plus, per clique, one reservation create, one snapshot create, one snapshot finalizer update, one reservation activation-status update, and one Active snapshot status update. The additional reservation validation GET makes the clique action count `N + 6*C`. A complete controller accounting for `D` ComputeDomains also includes one readiness-status write and its live GET per domain, for `N + 5*C + D` writes and `N + 6*C + 2*D` actions. Reads and idempotent actions remain separate from confirmed writes.

The controller uses one shared Node informer, indexed lookups by ComputeDomain, clique, Node, Pod UID, claim, and template, and keyed serialization for reservation creation and aggregate status. The current claim-attestation caches are cluster-wide. A disposable real-API gate measures controller HTTP and watch bytes, exact writes, conflicts, throttling, Active, and receipt-backed Ready at 18, 144, and 280x18. The Tier C runner separately collects exact workload Pod, reserved claim, timestamped kubelet NodePrepare, container Ready, and optional audit/data-plane evidence on real schedulable Nodes. The Tier D runner repeats the 280x18 virtual-node control-plane profile over digest-pinned stable Kubernetes patch images with version-matched clients and samples control-plane resource use. Local Kind has passed this matrix on the currently supported v1.34, v1.35, and v1.36 minors. Current two-Node AWS EKS availability can validate the real Tier C path directionally, but cannot supply the required 18/144-node end-to-end samples.

## Security boundary

Admission reserves provider markers, Node routing and attestation, snapshot and reservation writes, retirement evidence, protected RBAC bindings, and the persistent agent's narrow Pod receipt annotation. The controller accepts only the canonical generated ResourceClaimTemplate and allocated claim chain.

Alpha supports one installation in one fixed, admin-controlled namespace. The control namespace must not permit untrusted workloads to use the controller, kubelet, or agent ServiceAccounts. Safety resources are retained across ordinary Helm uninstall or gate changes; an unsafe decommission requires explicit operator action after verified zero state.

## Compatibility and rollout

Gate-off Kubernetes versions and clusters continue using the existing resource API conversion and per-domain path. Persistent mode requires `resource.k8s.io/v1`; this restriction does not raise the minimum Kubernetes version for the driver as a whole.

Rollout has four stages:

1. Upgrade to dual-capable binaries with the gate off.
2. Retire all existing ComputeDomains and verify the legacy daemon artifacts are gone.
3. Install admission and CRDs, then enable the gate and fleet.
4. Label one externally reset whole clique for a test ComputeDomain and run the complete formation, connectivity, retirement, and exact-ID reuse suite.

Rollback to the legacy provider is a decommission, not a gate toggle:

1. Stop new workload admission.
2. Retire every persistent ComputeDomain.
3. Verify no Active, Retiring, or quarantined snapshot remains and every reservation is Released.
4. Remove the retained fleet and persistent state under the documented decommission procedure.
5. Disable the gate and only then create legacy ComputeDomains.

## Test requirements

Before merge:

- API defaulting, strict decoding, generated client round trips, and CRD schema tests;
- fake-API formation, write-count, conflict, lost-response, restart, and retirement state-machine tests;
- race tests for controller, daemon, and kubelet packages;
- exact CLI parsing for `run --persistent-agent` and `check --persistent-agent`;
- Helm default compatibility and feature-on validation;
- real API-server admission tests, including teardown, second-install denial, and agent Pod-bound token writes;
- disposable real-API 18/144/280x18 formation with exact write, 409/429, watch-byte, Active, Ready, and steady-state no-op assertions;
- Kind validation of the T0-T3 analyzer, artifact schema, and promotion guards;
- executable real-node Tier C and multi-version virtual-node Tier D runners;
- allocator, hash, and indexed-reconcile benchmarks.

Before widening the alpha:

- genuine-fabric 2-node formation, IMEX connectivity, retirement, reboot evidence, and exact-ID reuse;
- 18-node and 144-node scheduler/kubelet/container/IMEX T0-T3 comparisons across supported capable stable Kubernetes minors;
- oldest/newest-capable-minor 280x18 informer/API-byte/control-plane profiles, paired with the largest available genuine-node run;
- planned agent maintenance and unplanned same-boot replacement exercises;
- gate-disable retirement with both healthy and quarantined members;
- clean decommission followed by legacy reuse.

## Acceptance criteria

The mode is ready for an alpha canary only when:

- gate-off behavior and supported Kubernetes versions remain unchanged;
- no per-ComputeDomain daemon artifact appears in persistent mode;
- workload release requires an exact installed snapshot receipt;
- partial or ambiguous membership never advances Active state;
- healthy deletion reaches Released and permits exact clique reuse;
- an unexpected same-boot agent replacement fails closed;
- disabling the gate does not prevent retirement of existing state;
- local, Kind admission, and genuine-fabric handoff suites pass;
- every unavailable scale claim remains labeled as unproven rather than inferred.
