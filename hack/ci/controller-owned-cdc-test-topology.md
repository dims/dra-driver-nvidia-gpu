# Test-only persistent GPU clique topology

The controller-owned clique tests need a topology source which survives a
compute-domain kubelet-plugin Pod replacement and a Node reboot. Hand-editing
`nvidia.com/gpu.clique` does not provide that contract: the next plugin process
reads NVML and correctly replaces the hand-written value with the hardware
result. On PCI-passthrough A100 test systems that result is commonly “fabric
not supported,” so a synthetic Active clique is quarantined even though the
controller and retirement state machines behaved correctly.

This repository therefore has a deliberately non-production topology provider.
It is compiled only with the `controllerownedcdctest` Go build tag. A normal
release binary contains no annotation reader and always uses NVML.

## Build a clearly test-only image

Use a unique image name or tag. Never reuse a release tag:

```bash
make -f deployments/container/Makefile build \
  ARCH=amd64 \
  IMAGE_NAME=local/dra-driver-nvidia-gpu \
  VERSION=controller-owned-cdc-topology-test-$(git rev-parse --short HEAD) \
  GO_BUILD_TAGS=controllerownedcdctest
```

The same provider can be compiled and tested without building an image:

```bash
make test-controller-owned-cdc-topology-provider
```

## Supply persistent topology

Before the tagged plugin starts, annotate every Node in the test clique with
the same synthetic clique ID:

```bash
kubectl annotate node node001 node002 \
  resource.nvidia.com/test-only-gpu-clique=krusty-clique-a --overwrite
```

The tagged binary reads this annotation each time it would otherwise query
NVML. The value persists across Pod replacement and Node reboot. Every use logs
a `TEST-ONLY synthetic GPU clique provider is active` warning with the Node and
value. Treat absence of that warning as a failed test setup.

Two sentinel values support negative tests:

- `<empty>` reports a successful discovery with no fabric clique;
- `<error>` reports a discovery failure.

Quote sentinel values in shell commands. Removing the annotation makes the
tagged binary delegate to NVML again:

```bash
kubectl annotate node node001 \
  resource.nvidia.com/test-only-gpu-clique-
```

Changing or removing the annotation while controller-v1 state is Active is a
deliberate topology-failure injection. It is expected to fail closed and may
quarantine the assignment. Do not use an Active fixture for a setup check.

## What this proves

This provider is appropriate for controller, admission, restart, reboot,
retirement-evidence, and reservation-reuse tests. It makes the synthetic
topology input durable enough that those tests are not invalidated by the next
plugin process.

It does **not** prove NVML/Fabric Manager behavior, real NVSwitch clique
discovery, IMEX data-plane health, or recovery from a genuine hardware topology
change. Those still require a real-fabric cluster. A test image, its annotation,
and any synthetic reservations must never be treated as production fence
evidence.

## Cleanup

After all synthetic fixtures have retired normally, restore a release image and
remove the test-only annotation. If the Node has an immutable startup topology
identity, follow the controller-owned retirement/fence workflow before changing
the annotation or routing labels; do not manually strip finalizers or retained
reservations merely to make the Node look clean.
