#!/usr/bin/env bash
# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the persistent-agent controller against a real, disposable Kubernetes
# API server. Virtual Nodes and allocated claims supply deterministic input;
# the production informer workers perform every measured controller write.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SHORT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short=12 HEAD)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SHORT_SHA}"
ARTIFACTS="${ARTIFACTS:-/tmp/persistent-agent-real-api-scale/${RUN_ID}}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-pa-real-api-${SHORT_SHA}}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
SCALE_SHAPES="${SCALE_SHAPES:-1x18 1x144 280x18}"
SCALE_FIXTURE_WORKERS="${SCALE_FIXTURE_WORKERS:-32}"
SCALE_TIMEOUT_SECONDS="${SCALE_TIMEOUT_SECONDS:-1800}"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"

for tool in docker go git kind kubectl tee uname; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done

mkdir -p "${ARTIFACTS}/audit"
export KUBECONFIG="${ARTIFACTS}/kubeconfig"

cleanup() {
  docker logs "${KIND_CLUSTER_NAME}-control-plane" > "${ARTIFACTS}/kind-control-plane.log" 2>&1 || true
  if [[ "${KEEP_CLUSTER}" != "true" ]]; then
    kind delete cluster --name "${KIND_CLUSTER_NAME}" > /dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cat > "${ARTIFACTS}/audit-policy.yaml" <<'EOF'
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
- level: RequestResponse
  resources:
  - group: resource.nvidia.com
    resources:
    - computedomains
    - computedomains/status
    - computedomaincliquereservations
    - computedomaincliquereservations/status
    - computedomaincliquesnapshots
    - computedomaincliquesnapshots/status
- level: Metadata
  resources:
  - group: ""
    resources:
    - nodes
- level: Metadata
EOF

cat > "${ARTIFACTS}/kind.yaml" <<EOF
apiVersion: kind.x-k8s.io/v1alpha4
kind: Cluster
networking:
  podSubnet: 10.0.0.0/8
nodes:
- role: control-plane
  extraMounts:
  - hostPath: ${ARTIFACTS}/audit-policy.yaml
    containerPath: /etc/kubernetes/audit-policy.yaml
    readOnly: true
  - hostPath: ${ARTIFACTS}/audit
    containerPath: /var/log/kubernetes
  kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
        audit-log-maxage: "1"
        audit-log-maxbackup: "1"
        audit-log-maxsize: "1000"
        audit-log-path: /var/log/kubernetes/audit.log
        audit-policy-file: /etc/kubernetes/audit-policy.yaml
      extraVolumes:
      - name: audit-policy
        hostPath: /etc/kubernetes/audit-policy.yaml
        mountPath: /etc/kubernetes/audit-policy.yaml
        pathType: File
        readOnly: true
      - name: audit-log
        hostPath: /var/log/kubernetes
        mountPath: /var/log/kubernetes
        pathType: DirectoryOrCreate
        readOnly: false
    controllerManager:
      extraArgs:
        controllers: "*,-node-ipam-controller,-node-lifecycle-controller"
        kube-api-burst: "2000"
        kube-api-qps: "1000"
        node-cidr-mask-size: "24"
EOF

{
  echo "run_id=${RUN_ID}"
  echo "commit=$(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "branch=$(git -C "${REPO_ROOT}" branch --show-current)"
  echo "dirty_files=$(git -C "${REPO_ROOT}" status --porcelain=v1 | wc -l | tr -d ' ')"
  echo "go=$(go version)"
  echo "kind=$(kind version)"
  echo "kubectl=$(kubectl version --client=true -o json | tr -d '\n')"
  echo "host=$(uname -a)"
  echo "node_image=${KIND_NODE_IMAGE}"
  echo "shapes=${SCALE_SHAPES}"
  echo "fixture_workers=${SCALE_FIXTURE_WORKERS}"
} > "${ARTIFACTS}/environment.txt"

kind delete cluster --name "${KIND_CLUSTER_NAME}" > /dev/null 2>&1 || true
kind create cluster \
  --name "${KIND_CLUSTER_NAME}" \
  --image "${KIND_NODE_IMAGE}" \
  --config "${ARTIFACTS}/kind.yaml" \
  --kubeconfig "${KUBECONFIG}" \
  --wait 120s 2>&1 | tee "${ARTIFACTS}/kind-create.log"

for crd in \
  resource.nvidia.com_computedomains.yaml \
  resource.nvidia.com_computedomaincliquesnapshots.yaml \
  resource.nvidia.com_computedomaincliquereservations.yaml; do
  kubectl apply -f "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu/crds/${crd}" > /dev/null
done
kubectl wait --for=condition=Established --timeout=60s \
  crd/computedomains.resource.nvidia.com \
  crd/computedomaincliquesnapshots.resource.nvidia.com \
  crd/computedomaincliquereservations.resource.nvidia.com > /dev/null

# Established means the CRD is served, but the watch cache may still return a
# transient "storage is (re)initializing" 429. Warm it before the measured
# controller starts so the zero-429 contract covers controller load, not CRD
# installation startup.
for resource in computedomaincliquesnapshots computedomaincliquereservations; do
  until kubectl get --raw "/apis/resource.nvidia.com/v1beta1/${resource}?watch=true&timeoutSeconds=1" > /dev/null 2>&1; do
    sleep 1
  done
done

kubectl get --raw /metrics > "${ARTIFACTS}/apiserver-before.prom"

cd "${REPO_ROOT}"
for shape in ${SCALE_SHAPES}; do
  cliques="${shape%x*}"
  members="${shape#*x}"
  if [[ -z "${cliques}" || -z "${members}" || "${shape}" != "${cliques}x${members}" ]]; then
    echo "ERROR: invalid shape '${shape}', expected CLIQUESxMEMBERS" >&2
    exit 1
  fi
  shape_artifacts="${ARTIFACTS}/${shape}"
  mkdir -p "${shape_artifacts}"
  kubectl get --raw /metrics > "${shape_artifacts}/apiserver-before.prom"
  PERSISTENT_AGENT_REAL_API_SCALE=1 \
    SCALE_CLIQUES="${cliques}" \
    SCALE_MEMBERS_PER_CLIQUE="${members}" \
    SCALE_FIXTURE_WORKERS="${SCALE_FIXTURE_WORKERS}" \
    SCALE_TIMEOUT_SECONDS="${SCALE_TIMEOUT_SECONDS}" \
    SCALE_RUN_ID="scale-${cliques}-${members}-${SHORT_SHA}" \
    SCALE_ARTIFACTS="${shape_artifacts}" \
    go test -mod=vendor -tags=integration ./cmd/compute-domain-controller \
      -run '^TestPersistentAgentRealAPIScale$' -count=1 \
      -timeout "$((SCALE_TIMEOUT_SECONDS + 300))s" -v \
      2>&1 | tee "${shape_artifacts}/test.log"
  kubectl get --raw /metrics > "${shape_artifacts}/apiserver-after.prom"
done

kubectl get --raw /metrics > "${ARTIFACTS}/apiserver-after.prom"
cp "${ARTIFACTS}/audit/audit.log" "${ARTIFACTS}/audit.log" 2>/dev/null || true

cat > "${ARTIFACTS}/README.md" <<EOF
# Persistent-agent real-API scale evidence

- Run: \`${RUN_ID}\`
- Commit: \`$(git rev-parse HEAD)\`
- Kubernetes: \`${KIND_NODE_IMAGE}\`
- Shapes: \`${SCALE_SHAPES}\`

This Tier B bundle runs the production informer workers and reconciliation
functions against a real disposable kube-apiserver. It records controller-side
HTTP request-body, response-body, and watch-body bytes, exact successful writes, 409/429
responses, production controller metrics, kube-apiserver metrics before and
after each shape, and an API audit log.

The virtual Nodes, Pods, and allocated ResourceClaims are deterministic test
inputs. Therefore this tier does not measure scheduler, kubelet
NodePrepareResources, container readiness, IMEX, or end-to-end T0-T3. Those
remain capable-cluster promotion work.
EOF

echo "PASS: persistent-agent real-API scale gate"
echo "Artifacts: ${ARTIFACTS}"
