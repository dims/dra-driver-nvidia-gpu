---
title: Upgrade
linkTitle: Upgrade
weight: 50
description: Upgrading the driver between releases.
---

This page covers upgrading the DRA Driver for NVIDIA GPUs between releases.

---

## Upgrade from v0.4.0 to v0.4.1

For the full release summary, see the [v0.4.1 release notes](https://github.com/kubernetes-sigs/dra-driver-nvidia-gpu/releases/tag/v0.4.1).

```bash
helm upgrade -i nvidia-dra-driver-gpu oci://registry.k8s.io/dra-driver-nvidia/charts/dra-driver-nvidia-gpu \
    --version {{< param "driver_version" >}} \
    --namespace nvidia-dra-driver-gpu \
    --set resources.gpus.enabled=false \
    --set gpuResourcesEnabledOverride=true
```

Append any additional `--set` flags you used at install time. For example, `--set nvidiaDriverRoot=/run/nvidia/driver` if the NVIDIA GPU Operator manages your drivers.

{{% alert title="Note" %}}
The `--set nameOverride=nvidia-dra-driver-gpu` flag is also needed if this is your first upgrade to v0.4.0 or later. Refer to [Upgrade from v25.12.0 to v0.4.0](#upgrade-from-v25120-to-v040) for more details on that flag.
{{% /alert %}}

After upgrading, confirm all driver pods are `Running` and `Ready`:

```bash
kubectl get pods -n nvidia-dra-driver-gpu
```

---

## Upgrade from v25.12.0 to v0.4.0

Upgrade the DRA Driver for NVIDIA GPUs from `v25.12.0` to `v0.4.0` without disrupting existing workloads. For the full release summary, see the [v0.4.0 release notes](https://github.com/kubernetes-sigs/dra-driver-nvidia-gpu/releases/tag/v0.4.0).

---

### What changed

The v0.4.0 release introduces several changes that affect how this upgrade is performed:

- The project moved from `NVIDIA/k8s-dra-driver-gpu` to `kubernetes-sigs/dra-driver-nvidia-gpu`. The Go module is now `sigs.k8s.io/nvidia-dra-driver-gpu` and container images are published to `registry.k8s.io/dra-driver-nvidia/dra-driver-nvidia-gpu` in addition to NVIDIA NGC Catalog (NGC).
- The Helm chart name changed from `nvidia-dra-driver-gpu` to `dra-driver-nvidia-gpu`. To keep existing Kubernetes resource names (DaemonSets, Deployments, ServiceAccounts, RBAC) stable, `--set nameOverride=nvidia-dra-driver-gpu` is required on the first upgrade. See [Upgrade procedure](#upgrade-procedure) below.
- In addition to NGC (`nvidia/dra-driver-nvidia-gpu`), the DRA Driver Helm chart is now also published to `oci://registry.k8s.io/dra-driver-nvidia/charts/dra-driver-nvidia-gpu`. You can continue to use the NGC chart or switch to the Kubernetes registry.
- Starting in v0.4.0, the chart follows SemVer and `--version 0.4.0` is required on `helm install` and `helm upgrade`.
- Once the cluster is on v0.4.0, downgrading to v25.12.0 is not supported. Two changes prevent downgrade: the kubelet plugin checkpoint format added a `BootID` field, and the `ComputeDomain` API now allows `numNodes` to be omitted. Plan this upgrade as forward-only. See the [v0.4.0 release notes](https://github.com/kubernetes-sigs/dra-driver-nvidia-gpu/releases/tag/v0.4.0) for more details.

### Before you begin

- Collect the `--set` flags you used at install time (for example, `gpuResourcesEnabledOverride`, `nvidiaDriverRoot`, `webhook.enabled`). You will pass the same flags on `helm upgrade`.
- If any node hit the "device cannot be reprepared after host reboot" issue ([#951](https://github.com/kubernetes-sigs/dra-driver-nvidia-gpu/issues/951)) prior to v0.4.0, remove the kubelet plugin checkpoint file on that node before upgrading. The new BootID-aware checkpoint format in v0.4.0 only invalidates checkpoints that already carry a recorded BootID; legacy checkpoints written by v25.12.0 are otherwise assumed valid.

### Upgrade procedure

Perform the following steps in order.

1. Update custom resource definitions.

Apply the v0.4.0 CRDs before upgrading the Helm chart. Helm only installs CRDs on first install and does not update them on `helm upgrade`, so applying them explicitly ensures the API schema is ready before the new controller and kubelet plugin start.

Update the ComputeDomains CRD:

```bash
kubectl apply \
    -f https://raw.githubusercontent.com/kubernetes-sigs/dra-driver-nvidia-gpu/refs/tags/v0.4.0/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomains.yaml
```

Update the ComputeDomainsCliques CRD:

```bash
kubectl apply \
    -f https://raw.githubusercontent.com/kubernetes-sigs/dra-driver-nvidia-gpu/refs/tags/v0.4.0/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomaincliques.yaml
```

For a release which contains `ControllerOwnedCDCliques`, download that exact
release's chart or source archive. Render and apply its admission policies and
bindings **before** applying the controller-owned ComputeDomainCliqueSnapshots,
cluster-scoped ComputeDomainCliqueReservations, and namespaced
ComputeDomainCliqueRetirementEvidences CRDs, and before upgrading
the controller or kubelet plugin binaries. Admission resource rules may safely
name a GVR which is not served yet; this order prevents a namespace writer from
pre-seeding controller-owned protocol or snapshot state in the CRD-to-binary
bootstrap interval. A CRD by itself does not reserve controller-owned metadata.
Helm does not install a newly added file from the chart's
`crds/` directory during `helm upgrade`:

```bash
RELEASE_VERSION=vX.Y.Z
curl -fsSLO "https://github.com/kubernetes-sigs/dra-driver-nvidia-gpu/archive/refs/tags/${RELEASE_VERSION}.tar.gz"
tar -xzf "${RELEASE_VERSION}.tar.gz"

# Render the exact release with the Phase-2 safety settings, then apply the
# policies and their bindings before either new CRD exists. Preserve/reapply
# the other values used by the installed release as appropriate.
helm template nvidia-dra-driver-gpu \
    "dra-driver-nvidia-gpu-${RELEASE_VERSION#v}/deployments/helm/dra-driver-nvidia-gpu" \
    --namespace nvidia-dra-driver-gpu \
    --api-versions resource.k8s.io/v1 \
    --set controllerOwnedCDCliques.admissionEnabled=true \
    --set featureGates.ControllerOwnedCDCliques=true \
    --set featureGates.CrashOnNVLinkFabricErrors=true \
    --set kubeletPlugin.containers.computeDomains.gpuCliqueLabelEnabled=true \
    --set controller.leaderElection.enabled=true \
    --set-json 'controllerOwnedCDCliques.canaryNamespaces=["my-canary-namespace"]' \
    --show-only templates/controllerownedcdc-installation.yaml \
    --show-only templates/validatingadmissionpolicy.yaml \
    --show-only templates/validatingadmissionpolicybinding.yaml \
    > controller-owned-admission.yaml
kubectl apply -f controller-owned-admission.yaml

# The immutable marker carries this release's Helm ownership metadata. The
# policies and bindings intentionally do not: use Helm v3.17 or newer and pass
# --take-ownership on the live install/upgrade so Helm adopts every pre-applied,
# release-neutral safety object. This is required again whenever a later chart
# adds a new policy or binding which the release has never owned. Keeping the
# flag on same-installation upgrades avoids having to predict that version diff.

kubectl apply -f "dra-driver-nvidia-gpu-${RELEASE_VERSION#v}/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomaincliquesnapshots.yaml"
kubectl apply -f "dra-driver-nvidia-gpu-${RELEASE_VERSION#v}/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomaincliquereservations.yaml"
kubectl apply -f "dra-driver-nvidia-gpu-${RELEASE_VERSION#v}/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomaincliqueretirementevidences.yaml"

kubectl wait --for=condition=Established \
    crd/computedomaincliquesnapshots.resource.nvidia.com
kubectl wait --for=condition=Established \
    crd/computedomaincliquereservations.resource.nvidia.com
kubectl wait --for=condition=Established \
    crd/computedomaincliqueretirementevidences.resource.nvidia.com

# Verify the served version and the snapshot status subresource. The dry-run
# must pass authorization and schema routing without creating an object.
kubectl get --raw /apis/resource.nvidia.com/v1beta1 | \
    grep -q 'computedomaincliquesnapshots/status'
kubectl get --raw /apis/resource.nvidia.com/v1beta1 | \
    grep -q 'computedomaincliqueretirementevidences'
kubectl auth can-i update computedomaincliquesnapshots/status \
    --api-group=resource.nvidia.com \
    --as=system:serviceaccount:nvidia-dra-driver-gpu:nvidia-dra-driver-gpu-controller \
    -n nvidia-dra-driver-gpu
```

Verify that the rendered `computedomain-protocol-policy`, reserved-metadata,
Node-topology, reservation-writer, snapshot-writer, and retirement-evidence
policies, plus `controllerownedcdc-lifecycle-policy`, all have active
`ValidatingAdmissionPolicyBinding` objects before continuing. The controller
also rejects a persisted protocol marker which predates its finalizer, but the
admission layer is required to prevent that invalid state rather than merely
fail closed after it is stored.

Leave `ControllerOwnedCDCliques=false` until the CRD is Established and the
dual-capable kubelet plugin has rolled out to every eligible node. Then opt in
only a canary ComputeDomain by creating it with
`resource.nvidia.com/requestedComputeDomainCliqueProtocol: controller-v1` and
a positive `spec.numNodes` equal to the complete expected Node count. Before
that creation, reserve the **entire physical clique** for the canary and use
hardware that is either new or has been externally quiesced and reset. Prevent
every legacy workload from beginning Prepare on that clique throughout
controller-mode formation. The legacy path can check whether a reservation
already exists, but only the later controller reconciliation atomically creates
the reservation; those operations do not form one cross-protocol transaction.
Deleting a legacy Kubernetes object is not proof that its old IMEX runtime
stopped, so strict v1 does not treat ordinary legacy deletion as migration
fence evidence. Enable leader election and topology publication and explicitly
allowlist an operator-controlled canary namespace:

```yaml
featureGates:
  ControllerOwnedCDCliques: true
  CrashOnNVLinkFabricErrors: true
kubeletPlugin:
  containers:
    computeDomains:
      gpuCliqueLabelEnabled: true
controller:
  leaderElection:
    enabled: true
controllerOwnedCDCliques:
  admissionEnabled: true
  canaryNamespaces:
  - my-canary-namespace
```

Alpha requires Kubernetes v1.34 or newer with the served
`resource.k8s.io/v1` API. Kubernetes v1.32/v1.33 beta DRA APIs remain supported
by legacy-v1 only. Alpha supports one chart installation in one fixed primary
driver namespace and is not supported on OpenShift until SCC bindings are
covered by the same immutable admission boundary.
`CrashOnNVLinkFabricErrors` must remain enabled while controller-v1 state
exists. A controller-owned Node must fail closed when NVML cannot verify its
startup fabric identity; silently falling back to non-fabric mode would make a
temporary observation indistinguishable from an authorized topology change.
Do not install another release, rename or move the controller ServiceAccount,
or migrate the control namespace while controller-owned admission policies or
state exist. The policies are cluster-scoped and authorize the exact release
ServiceAccount identities; multiple releases would overwrite or conjunctively
conflict with one another. Additional driver namespaces remain legacy-only.

The chart enforces this boundary with a fixed, zero-permission ClusterRole
named `controller-owned-cdc-installation.dra-driver-nvidia-gpu`. It records the
Helm release, primary control namespace, controller ServiceAccount, and kubelet
plugin ServiceAccount as immutable annotations. It also records the exact
pre-controller-v1 controller and kubelet Role and binding names for that same
release and namespace. Those legacy aliases permit a verified zero-state
rollback without permitting a different release or ServiceAccount to acquire
the protected roles. The marker carries the canary namespace allowlist as
parameters for every safety policy. `helm install`/`helm upgrade` uses a live
lookup and rejects a different release or namespace before rendering
namespaced resources. The installation policy denies identity changes or
deletion of the ClusterRole marker. The controller checks the recorded identity
at startup. Client-only `helm template` cannot perform a live lookup.
Do not use `helm template | kubectl apply` to install or upgrade the chart:
multi-document apply is not transactional. The retained RBAC-binding policy
prevents an offline second render from transferring the protected controller or
kubelet ClusterRoles and the protected namespaced Roles/RoleBindings. The
policies and bindings omit release-specific Helm ownership annotations from
offline renders, so the second render also cannot take ownership of the
retained admission boundary. It may still leave unprivileged partial objects.
Use live `helm upgrade --atomic --take-ownership` after the admission-first
bootstrap. The flag is required for the first adoption and again whenever a
later chart adds a release-neutral policy or binding which this release has
never owned. Keeping it on same-installation upgrades is the simpler safe
procedure; the immutable marker and live identity checks still reject another
release or control namespace.

Before enabling `admissionEnabled` on brownfield, inventory and remove every
other driver release, controller/kubelet workload, and associated RBAC. The
guard prevents a new competing installation; it does not revoke privileges
which a different legacy release already holds. The primary control namespace
is a trusted administrative namespace: do not grant tenants permission to
create Pods, Jobs, ReplicaSets, or other workloads using the protected ServiceAccounts.
Also inventory the three new GVRs and reserved ComputeDomain/Node metadata before
enabling admission. A prior default install may have installed the CRDs while
the opt-in policies were disabled; preexisting protocol markers, reservations,
snapshots, isolation labels, or controller attestations must be absent or
explicitly audited before Phase 2.

Upgrading the existing release in place with the same release name, namespace,
and ServiceAccount names is supported. The first upgrade which introduces this
guard must render `controllerownedcdc-installation.yaml` before the policies
and bindings; it creates the installation ClusterRole and changes the policies to
read their subjects and canary namespaces from it. Those resources carry
`helm.sh/resource-policy: keep`, so Helm retains the safety boundary on
uninstall or rollback. Do not change any value which changes a marker-recorded
identity: `nameOverride`/`fullnameOverride` when they alter protected workload
names, `serviceAccount.name`, `namespaceOverride`, the Helm release name, or the
Helm namespace. A purely cosmetic override which leaves every recorded
ServiceAccount, workload, Role, and binding name identical is not a distinct
installation identity.

There is intentionally no ordinary Helm uninstall, old-chart rollback, or
namespace-migration path while alpha controller-v1 state exists. When the
cluster has no controller-v1 ComputeDomains, snapshots, retirement evidence,
or unreleased reservations, an in-place rollback of the same release in the
same namespace to the recorded legacy workload and RBAC names is supported.
The retained policies continue to reject any other release, namespace, or
ServiceAccount during and after that rollback. Test that exact chart transition
on the target Kubernetes version before relying on it as a recovery path.

The lifecycle admission policy rejects deletion of the protected controller
Deployment, kubelet-plugin DaemonSet, and NVIDIA ComputeDomain CRDs until the
installation marker carries the explicit unsafe-decommission approval. This
makes an ordinary `helm uninstall` fail closed while the controller and API
state remain available; it does not turn approval into runtime fence evidence.

A full decommission or any rollback with controller-owned state still requires
an explicit reviewed procedure which must
first inventory and remove every controller-v1 ComputeDomain and snapshot,
verify the whole-clique reset/fence, and account for retained reservations. Only
then may a cluster administrator mark the protected admission objects with
`resource.nvidia.com/unsafe-controller-owned-cdc-decommission=approved` and
remove the binding, policies, and retained CRDs/state in a reviewed order. The
workload and RBAC policies permit the exact marker-recorded legacy controller
and kubelet identities so the admission-first brownfield rollout can overlap
old binaries and a verified zero-state rollback can complete. This is not
permission to roll back while a controller-owned clique exists. A rollback to
a chart predating controller-v1 requires verified zero controller state and the
recorded same-installation transition, or the reviewed decommission first. That
annotation is an unsafe administrative authorization, not fence evidence.

Only trusted operators should be able to create ComputeDomains in an allowed
canary namespace. For controller-v1, the controller derives Node membership
from an exact live scheduled Pod, its generated and allocated ResourceClaim,
the Pod UID-qualified `reservedFor` entry and Node selection, the canonical
ResourceClaimTemplate, and the persisted protocol. It then writes a UID-bearing
Node attestation; the snapshot expected set ignores an unattested label.
Legacy-v1 keeps the old kubelet-written label for brownfield and rollback
compatibility, but it cannot authorize controller-v1 membership. Use an entire
virgin or externally reset clique and keep legacy scheduling excluded because
claim-derived attestation happens after scheduling and is not an atomic
cross-protocol scheduling lock.

Keep leader election and topology publication enabled while any persisted
controller-v1 ComputeDomain exists, including after disabling the feature gate
for new admission. Normal deletion of a healthy controller-v1 ComputeDomain is
evidence-bearing: first terminate its workload Pods, then delete the
ComputeDomain. The controller retains the exact daemon Pods and Node routes,
changes each snapshot from `Active` to `Retiring`, and waits while every daemon
stops and reaps its supervised IMEX child and publishes immutable, Pod-bound
retirement evidence. If the original daemon Pod was lost in a real Node reboot,
a replacement daemon may instead publish `NodeReboot` evidence only when the
snapshot's activation boot ID and the live Node boot ID are both nonempty and
different. Same-boot Pod replacement remains blocked. Evidence is stored in a
ComputeDomainCliqueRetirementEvidence object rather than on the witness Pod, so
later Pod deletion cannot erase a verified fence. The controller then records
`Fenced`, durably marks the reservation `Released`, removes the evidence,
snapshot, and runtime objects, and clears the ComputeDomain finalizer. Watch
`status.conditions[type=CliqueRetirementReady]` while deletion is in progress.

Do not force-delete daemon Pods, remove Node routing labels, or strip snapshot,
reservation, evidence, or ComputeDomain finalizers during this sequence.
Pod/Node absence, timeout, `NotReady`, and object deletion are not fence
evidence. A same-boot replacement cannot stand in for the published daemon. A
snapshot created before member boot IDs were recorded also cannot use
`NodeReboot` evidence and still requires an externally verified whole-clique
reset/recovery procedure. After every member is fenced, the controller writes a
one-shot Node retirement-fence marker; a topology-invalid kubelet plugin may
consume that marker to clear the stale startup identity and republish freshly
discovered topology. Do not recreate the clique as legacy-v1 as a shortcut.
Helm rollback does not roll CRD schemas back. Existing ComputeDomains retain
their persisted protocol; disabling the feature gate stops new controller-v1
admission but does not stop reconciliation of active controller-v1 domains.
Do not uninstall the chart or roll back to a chart/binary which predates
controller-v1 while any reservation or controller-v1 ComputeDomain exists.
The admission policies are normal chart resources while CRDs and strict
tombstones outlive an uninstall; removing those policies would remove the
writer/delete guard from retained state, and old binaries do not understand
the protocol, retirement evidence, or reservation fence. Likewise, do not switch
`imex.mode` from `driverManaged` to `hostManaged` until every controller-v1
domain has gone through the verified whole-fabric recovery procedure; current
binaries refuse that transition while controller-owned state remains.

2. Upgrade the Helm chart by using the `helm upgrade -i` command to upgrade the chart in place. 

Two flags are required to upgrade to v0.4.0:

- `--version 0.4.0`, because v0.4.0 introduces SemVer.
- `--set nameOverride=nvidia-dra-driver-gpu`, because the chart was renamed. Without this override, the new chart creates duplicate Kubernetes objects (kubelet plugin DaemonSet, controller Deployment, RBAC, and so on) under the new name instead of upgrading the existing ones. This is only required on the first upgrade to v0.4.0 or later.

The following command upgrades the chart and switches to using the Kubernetes registry source:

```bash
helm upgrade -i nvidia-dra-driver-gpu oci://registry.k8s.io/dra-driver-nvidia/charts/dra-driver-nvidia-gpu \
    --version 0.4.0 \
    --namespace nvidia-dra-driver-gpu \
    --set gpuResourcesEnabledOverride=true \
    --set nameOverride=nvidia-dra-driver-gpu
```

Append any additional `--set` flags you used at install time. For example, `--set nvidiaDriverRoot=/run/nvidia/driver` if the NVIDIA GPU Operator manages your drivers.

Example output:

```
Release "nvidia-dra-driver-gpu" has been upgraded. Happy Helming!
NAME: nvidia-dra-driver-gpu
LAST DEPLOYED: Wed May 13 05:30:39 2026
NAMESPACE: nvidia-dra-driver-gpu
STATUS: deployed
REVISION: 2
TEST SUITE: None
```

{{% alert title="Note" %}}
Subsequent `helm upgrade` calls do not need `--set nameOverride=nvidia-dra-driver-gpu`. It is only required on the first upgrade to v0.4.0 or later.
{{% /alert %}}

### Verify the upgrade

After `helm upgrade` completes, confirm the new pods are running and pre-existing workloads are still healthy.

1. Check that all driver pods are `Running` and `Ready`:

```bash
kubectl get pods -n nvidia-dra-driver-gpu
```

Example output:

```
NAME                                                READY   STATUS    RESTARTS   AGE
nvidia-dra-driver-gpu-controller-5c968c745f-s8n2m   1/1     Running   0          13s
nvidia-dra-driver-gpu-kubelet-plugin-6fmmd          2/2     Running   0          112s
```

The `controller` pod runs the ComputeDomain controller. The `kubelet-plugin` pod runs two containers (`gpus` and `compute-domains`) when both resource plugins are enabled, so it reports `2/2`.

2. Confirm every pre-existing `ResourceClaim` is still allocated and reserved for its pod:

```bash
kubectl get resourceclaims -A
```

Example output, showing a ComputeDomain workload and two GPU workloads that were running before the upgrade:

```
NAMESPACE               NAME                                                           STATE                AGE
default                 imex-channel-injection-imex-channel-0-c5pnt                    allocated,reserved   13s
nvidia-dra-driver-gpu   computedomain-daemon-dc84d905-2336-45fa-9-compute-domaifb5sg   allocated,reserved   12s
gpu-test1               pod1-gpu-7r7zn                                                 allocated,reserved   119s
gpu-test1               pod2-gpu-stmv8                                                 allocated,reserved   119s
```

Every claim that was bound before the upgrade should still report `allocated,reserved`.

### Troubleshooting

If a workload pod is in a non-`Running` state after the upgrade, capture the kubelet plugin logs from the node where the pod was scheduled:

```bash
kubectl logs -n nvidia-dra-driver-gpu <kubelet-plugin-pod>
```

If you see `checkpoint is corrupted` errors, the v0.4.0 kubelet plugin now logs a diff between the on-disk and re-marshaled checkpoint contents to make this easier to debug. Include that log output when filing an [issue](https://github.com/kubernetes-sigs/dra-driver-nvidia-gpu/issues).
