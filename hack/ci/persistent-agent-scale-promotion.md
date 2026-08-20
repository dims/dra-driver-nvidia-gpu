# Persistent-agent Tier C and Tier D execution guide

This is the executable handoff for the remaining scale-promotion work. It is
deliberately aligned with the environments currently available:

| Environment | What it can prove now | What it cannot prove |
|---|---|---|
| Local Kind on the development machine | Timeline analyzer, artifact schema, promotion guards, real API/informer/workqueue behavior, and a v1.34 280×18 virtual-node control-plane profile | Real `NodePrepareResources`, containers using NVIDIA devices, NVML, IMEX, or genuine T0–T3 |
| Current AWS EKS `yljtrxpmzu`, two GB200 Nodes in one real clique | Scheduler, real kubelet `NodePrepareResources`, container readiness, persistent agent, IMEX/nvbandwidth, retirement, and directional two-node T0–T3 | The required 18- and 144-node sample sizes or a 5,040-node control plane |
| Future 18/144 capable fleet | Tier C promotion evidence | Tier D 5,040-node pressure unless paired with the virtual control-plane rig |

The scripts refuse to promote a two-node result. Do not override that boundary
or merge Kind and real-hardware numbers into one synthetic result.

## 1. Local validation available now

Run the analyzer unit tests and shell checks:

```bash
go test ./hack/tools/persistent-agent-timeline ./hack/tools/persistent-agent-comparison -count=1
shellcheck \
  hack/ci/persistent-agent-tier-c-lib.sh \
  hack/ci/persistent-agent-tier-c-test.sh \
  hack/ci/persistent-agent-tier-c-tooling-smoke-test.sh \
  hack/ci/persistent-agent-tier-c-kind-smoke-test.sh \
  hack/ci/persistent-agent-tier-c-nvbandwidth.sh \
  hack/ci/persistent-agent-tier-d-test.sh \
  hack/ci/persistent-agent-two-node-performance.sh \
  hack/ci/persistent-agent-two-node-performance-smoke-test.sh \
  hack/ci/persistent-agent-two-node-smoke.sh
make test-persistent-agent-two-node-performance-smoke
```

The smoke target includes regressions for long MPIJob-derived names, transient
log collection failures, and workload Pods completing while the device smoke
is in progress. The real Tier C runner waits separately for the ComputeDomain
to become Ready and keeps the default 1 KiB nvbandwidth payload alive for 2,000
samples so that status convergence can be observed before MPI cleanup.

Run the Kind Tier C smoke. It uses real Pod conditions but synthetic claims and
NodePrepare timestamps, so its generated README labels the result correctly:

```bash
ARTIFACTS=/tmp/pa-tier-c-kind-smoke \
  make test-persistent-agent-tier-c-kind-smoke
```

Run a short Tier D orchestration smoke:

```bash
ARTIFACTS=/tmp/pa-tier-d-18 \
TIER_D_SHAPE=1x18 \
TIER_D_KIND_NODE_IMAGES=kindest/node:v1.34.0 \
  make test-persistent-agent-tier-d
```

Run the full locally available 5,040-node profile:

```bash
ARTIFACTS=/tmp/pa-tier-d-280x18 \
TIER_D_SHAPE=280x18 \
TIER_D_KIND_NODE_IMAGES=kindest/node:v1.34.0 \
  make test-persistent-agent-tier-d
```

This must retain exact `6,722` actions / `6,441` writes, zero 409s, zero 429s,
and a complete `matrix.csv`. It is a v1.34 checkpoint, not the final supported-
minor matrix.

The developer checkpoint also passed the complete digest-pinned, version-
matched v1.34.8/v1.35.5/v1.36.1 matrix on 2026-08-17:

| Kubernetes | Active | Ready | Actions / writes | 409 / 429 |
|---|---:|---:|---:|---:|
| v1.34.8 | 19.651s | 28.706s | 6,722 / 6,441 | 0 / 0 |
| v1.35.5 | 17.869s | 26.908s | 6,722 / 6,441 | 0 / 0 |
| v1.36.1 | 20.814s | 29.875s | 6,722 / 6,441 | 0 / 0 |

Those timings are observational single runs. QA must independently reproduce
the matrix; exact counts and zero conflicts/throttling are the hard assertions.

## 2. Main versus latest-branch two-Node performance study

This study has exactly two subjects:

- `M`: an actual, clean, pinned upstream `main` checkout and its image/chart;
- `B`: the clean, GPG-signed tip of the persistent-agent branch.

Do not substitute the branch with its feature gate disabled for `M`. The
runner knows that actual `main` has no persisted clique-protocol annotation and
requires that annotation to remain absent. It requires
`persistent-agent-v1` only for `B`.

The orchestrator runs four balanced blocks (`M,B`, `B,M`, `M,B`, `B,M`). Each
arm in each block gets two excluded warm-ups, 25 cold-domain lifecycle cycles,
and 25 warm-workload cycles. That produces 100 measured cold cycles and 100
measured warm cycles per subject. Every cold cycle records T0–T3 plus D0–D4,
keeps watches active through teardown, and refuses manual cleanup. Every cycle
runs a cheap `nvidia-smi` device smoke; the default cadence runs the heavier
nvbandwidth check on the first, middle, and final measured cycle of each
session.

Three executable environment-specific hooks are mandatory:

- `PERF_MAIN_INSTALL_HOOK` installs the exact source and image supplied in
  `PERF_SOURCE_WORKTREE`, `PERF_SOURCE_SHA`, and `PERF_DRIVER_IMAGE` and waits
  for the ordinary main driver to be Ready;
- `PERF_BRANCH_INSTALL_HOOK` follows the guarded persistent-agent installation
  procedure and waits for the controller, kubelet plugin, and exactly one
  agent on each selected Node to be Ready;
- `PERF_DECOMMISSION_HOOK` fully retires provider state and removes the driver,
  CRDs/admission objects when applicable, Node protocol metadata, and the
  leader-election Lease. It must leave the shared MPI Operator installed.

Each hook also receives `PERF_ACTION`, `PERF_ARM`, `PERF_BLOCK`,
`PERF_DRIVER_NAMESPACE`, and `PERF_SESSION_ARTIFACTS`. Save rendered manifests,
Helm values, image-build provenance, runtime image IDs, and cleanup proof in
that artifact directory. A hook failure is a hard stop: the orchestrator
preserves the installation and does not attempt an automatic decommission.

First run the five-cycle pilot:

```bash
ARTIFACTS=/path/to/artifacts/main-vs-branch-pilot \
PERF_PILOT=true \
PERF_MAIN_WORKTREE=/path/to/clean/main \
PERF_MAIN_SHA=$(git -C /path/to/clean/main rev-parse HEAD) \
PERF_BRANCH_WORKTREE="$PWD" \
PERF_MAIN_DRIVER_IMAGE='<main image identity>' \
PERF_BRANCH_DRIVER_IMAGE='<branch image identity>' \
PERF_MAIN_INSTALL_HOOK=/path/to/install-main.sh \
PERF_BRANCH_INSTALL_HOOK=/path/to/install-branch.sh \
PERF_DECOMMISSION_HOOK=/path/to/decommission-driver.sh \
PERF_NODE_SELECTOR='scale-promotion.nvidia.com/tier-c=true' \
NVB_GPUS_PER_NODE=2 \
  make test-persistent-agent-two-node-performance
```

The pilot deliberately does not enforce performance thresholds. Inspect exact
source/image identities, measured/warm-up separation, `lifecycle.json`, watch
scope, nvbandwidth, and pristine decommission evidence before the full run.
Then repeat without `PERF_PILOT=true`; the full run enforces the documented
20% cold improvement, 5% warm non-regression, paired block confidence
interval, no branch per-domain DaemonSet creation, D0–D4 bound, sample counts,
and first-to-last-quartile drift.

On the first installation of each arm the orchestrator also samples driver Pod
CPU/memory with `kubectl top --containers` every five seconds for 15 minutes.
The pilot caps this at ten seconds. If metrics.k8s.io or authorization is not
available, the artifact is explicitly marked unavailable rather than treated
as zero. Override `PERF_IDLE_SECONDS` or `PERF_IDLE_SAMPLE_SECONDS` only before
the run and record the reason.

The final outputs are `manifest.csv`, raw block/session/cycle artifacts, and
`comparison/{comparison.json,comparison.csv,report.md}`. Client-visible watch
bytes are not presented as total apiserver bytes. Two-Node repetition remains
directional evidence and does not replace the 18/144-Node Tier C gate.

## 3. AWS EKS two-node Tier C mini-run

Start from the pristine-cluster and signed-source requirements in the existing
persistent-agent QA handoff. Install one subject at a time. Actual main and the
persistent-agent branch cannot coexist on the same Nodes.

Label exactly the two intended GB200 Nodes with an ordinary test-selection
label. This is not the persistent-agent isolation label; the runner applies and
later verifies cleanup of that protocol label itself. The selector must be one
exact `key=value` expression because the workload fixture enforces it as a
Node selector. Required Pod anti-affinity enforces one workload Pod per host,
and preflight verifies the Nodes form the declared physical-clique shape.

```bash
kubectl label node <node-a> <node-b> scale-promotion.nvidia.com/tier-c=true
```

For a real persistent-agent nvbandwidth trial:

```bash
ARTIFACTS=/path/to/artifacts/persistent-1x2 \
TIER_C_PROVIDER=persistent-agent-v1 \
TIER_C_SHAPE=1x2 \
TIER_C_TRIALS=5 \
TIER_C_PROMOTION_RUN=false \
TIER_C_T0_MODE=creationTimestamp \
TIER_C_PROFILE=directional \
TIER_C_CACHE_STATE=unspecified \
TIER_C_NODE_SELECTOR='scale-promotion.nvidia.com/tier-c=true' \
TIER_C_WORKLOAD_TEMPLATE=hack/ci/fixtures/persistent-agent-tier-c/nvbandwidth-workload.yaml.tmpl \
TIER_C_DATA_PLANE_SCRIPT="$PWD/hack/ci/persistent-agent-tier-c-nvbandwidth.sh" \
NVB_GPUS_PER_NODE=2 \
  make test-persistent-agent-tier-c
```

`creationTimestamp` is an explicit fallback because the present AWS EKS
workflow does not expose an apiserver audit log to the test runner. The result
is directional and must say so. If an audit JSONL export becomes available,
set `TIER_C_T0_MODE=audit` and `TIER_C_AUDIT_LOG=/path/to/audit.jsonl`.

For the matching main sample, fully retire and decommission the persistent
fleet first. Install the exact pinned upstream-main source/image, prove there
are zero persistent-agent Pods, and run the same command with:

```bash
TIER_C_PROVIDER=main
TIER_C_SOURCE_WORKTREE=/path/to/clean/pinned-main
```

Keep Kubernetes version, worker Nodes, image digests, image cache state,
nvbandwidth parameters, controller limits, and workload template identical.
The runner rejects a main trial if a persistent-agent Pod remains anywhere in
the driver namespace.

The output contains raw Pods, claims, Nodes, snapshots, reservations, Events,
timestamped compute-domain kubelet-plugin logs, per-trial `timeline.csv` and
`result.json`, driver logs, resource snapshots, the nvbandwidth log, aggregate
percentiles, and deterministic trial-cluster bootstrap 95% confidence
intervals. The result records the bootstrap seed and repetition count. Confirm
the nvbandwidth log reports `multinode_device_to_device_memcpy_read_ce` and no
error/failure marker.

By default the runner discovers kubelet-plugin Pods by their `compute-domains`
container in the configured driver namespace. It does not hard-code the Helm
component-label prefix because that key changes with `nameOverride`. Preflight
requires exactly one Ready matching Pod on every selected Node, so an empty log
collection cannot silently proceed. `TIER_C_KUBELET_SELECTOR` remains an
optional override for unusual installations.

Five two-node trials are useful for finding harness or correctness defects.
They are not enough for p95/p99 or promotion.

The independent 2026-08-17 AWS EKS checkpoint completed all five persistent-
agent trials on two real GB200 Nodes after the evidence-check fixes: every
trial reached Active/Ready, every nvbandwidth hook passed at about 825 GB/s
across all four GPU pairs, and aggregate T0–T3 was 6.0s p50 / 19.0s p95 with
NodePrepare at 5.18s p50. This confirms the real path directionally; it does
not change the 18/144-node promotion requirement.

The committed auto-discovery path was subsequently reverified from a clean
checkout: the source record reported `auto:compute-domains-container`, the
kubelet-plugin log was nonempty, exact T2 evidence and nvbandwidth passed, and
cleanup was automatic. Earlier comparison timing is retained only in the
historical work log and is not an upstream-main baseline. New comparative
claims must come from the two-subject M/B harness above.

## 4. Tier C promotion run when 18/144 Nodes are available

The current two-Node cluster needs 16 additional simultaneously schedulable,
fabric-compatible Nodes to run the 18-Node milestone. Full Tier C requires 142
additional Nodes, for 144 total. The same 144-Node fleet can run both shapes by
selecting 18 Nodes for the smaller case; separate 18- and 144-Node clusters are
not required. Spare Nodes outside the required physical-clique/fabric shape do
not count.

Use the same runner with one physical clique per trial:

```bash
TIER_C_PROMOTION_RUN=true
TIER_C_T0_MODE=audit
TIER_C_AUDIT_LOG=/path/to/audit.jsonl
TIER_C_CLOCK_SKEW_FILE=/path/to/measured-clock-skew.txt
TIER_C_TRIALS=30
TIER_C_SHAPE=1x18        # repeat separately with 1x144
TIER_C_DATA_PLANE_SCRIPT="$PWD/hack/ci/persistent-agent-tier-c-nvbandwidth.sh"
TIER_C_OBSERVABILITY_SCRIPT=/path/to/cluster-specific-metrics-collector.sh
```

For the persistent run also set `TIER_C_PROFILE=persistent-warm` and provide
`TIER_C_FLEET_WARMUP_FILE` containing the separately measured fleet install-to-
Ready interval. For actual main use `TIER_C_PROVIDER=main` with
`TIER_C_PROFILE=main-default` and `TIER_C_PROFILE=main-tuned` in separate clean
runs. Run every profile once
with `TIER_C_CACHE_STATE=warm` and once with `cold`; prepare the image cache
outside the runner and record the exact procedure. The runner records these
claims but does not pretend to flush a runtime's image cache itself.

The observability hook is invoked once with
`TIER_C_OBSERVABILITY_PHASE=before` and once with `after`, plus the trial
namespace, ID, and artifact directory. It must collect the environment-specific
controller, scheduler, etcd, queue, API Priority and Fairness, and component
CPU/memory evidence that cannot be reached portably from every cluster. A
promotion run refuses to proceed without this executable hook; a directional
two-node AWS EKS mini-run may omit it and will still retain the generic apiserver metrics,
driver logs, Events, and best-effort `kubectl top` output.

Run the profiles separately on every supported stable Kubernetes minor which
serves the required `resource.k8s.io/v1` API. As of 2026-08-17 the upstream-
supported minors are v1.34, v1.35, and v1.36. Re-evaluate this list at execution
time instead of hard-coding it into the product. The runner requires exact Node
selection, exact Pod count, one workload Pod per Node, the expected persisted
provider, ComputeDomain Ready, exact snapshot count, exact claim UID in the
successful NodePrepare log, and ordered T0≤T1≤T2≤T3.

Thirty trials permit p50/p95 reporting. Do not claim p99 without several
hundred samples. Report fleet installation separately from warm workload
formation.

## 5. Tier D final matrix

The final control-plane command must name digest-pinned stable patch images for
the supported capable Kubernetes minors. Do not use `latest`, a prerelease, or
unreleased Kubernetes v1.37 as promotion evidence. With Kind v0.32.0, the
currently published upstream images are:

```text
kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256
kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95
kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5
```

Re-resolve official image digests when the matrix is run; these are a dated
checkpoint, not a permanent version policy.

```bash
ARTIFACTS=/path/to/artifacts/tier-d \
TIER_D_PROMOTION_RUN=true \
TIER_D_SHAPE=280x18 \
TIER_D_KIND_NODE_IMAGES="$V134_IMAGE $V135_IMAGE $V136_IMAGE" \
  make test-persistent-agent-tier-d
```

Add an intervening stable minor if it materially changes DRA or scheduler
behavior. Each version gets the complete real-API bundle, audit log, apiserver
metrics, exact request/response/watch bytes, Kind logs, and sampled control-
plane container CPU/memory. The wrapper downloads a matching `kubectl`, checks
its published SHA-256, and saves its version with each result. The top-level
`matrix.csv` must show zero conflicts and throttling at every version.

This virtual-node result is paired with, not substituted for, the largest real
Tier C real-hardware run. The final promotion decision needs both.

## 6. Hard stops and cleanup

Stop and preserve the fixture on any of these:

- missing or out-of-order T0–T3 milestone;
- workload scheduled outside the selected Node set;
- fewer unique Nodes than workload Pods;
- wrong persisted provider or ComputeDomain not Ready;
- wrong persistent snapshot count;
- successful NodePrepare log not matching the exact reserved claim UID;
- data-plane failure;
- controller 409/429 during healthy Tier D formation;
- stuck retirement or isolation label after deletion.

The Tier C runner preserves a failed namespace by default. Successful trials
delete their workload and ComputeDomain, wait for retirement, verify isolation
cleanup, and delete the trial namespace. The Tier D runner preserves a failed
Kind cluster by default and deletes successful clusters.
