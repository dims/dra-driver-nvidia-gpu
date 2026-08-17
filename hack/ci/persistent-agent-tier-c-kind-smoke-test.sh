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

# Validates the Tier C timeline and artifact contract with real Kubernetes Pod
# conditions. NodePrepare and claims are synthetic: Kind has no genuine NVIDIA
# DRA/IMEX path, so this can never be reported as Tier C promotion evidence.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SHORT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short=12 HEAD)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-${SHORT_SHA}}"
ARTIFACTS="${ARTIFACTS:-/tmp/persistent-agent-tier-c-kind-smoke/${RUN_ID}}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-pa-tier-c-smoke-${SHORT_SHA}}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"

for tool in docker envsubst git go jq kind kubectl yq; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done

mkdir -p "${ARTIFACTS}"
export KUBECONFIG="${ARTIFACTS}/kubeconfig"

cleanup() {
  kind export logs "${ARTIFACTS}/kind-logs" --name "${KIND_CLUSTER_NAME}" > /dev/null 2>&1 || true
  kind delete cluster --name "${KIND_CLUSTER_NAME}" > /dev/null 2>&1 || true
}
trap cleanup EXIT

kind delete cluster --name "${KIND_CLUSTER_NAME}" > /dev/null 2>&1 || true
kind create cluster --name "${KIND_CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}" --kubeconfig "${KUBECONFIG}" --wait 120s > "${ARTIFACTS}/kind-create.log" 2>&1
kubectl create namespace pa-tier-c-smoke > /dev/null
kubectl apply -f "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomains.yaml" > /dev/null
kubectl wait --for=condition=Established --timeout=60s crd/computedomains.resource.nvidia.com > /dev/null

export TRIAL_ID=kind-template-smoke
export TRIAL_NAMESPACE=pa-tier-c-smoke
export EXPECTED_PODS=2
export COMPUTE_DOMAIN_NAME=cd-kind-template-smoke
export CHANNEL_TEMPLATE_NAME=channel-kind-template-smoke
export WORKLOAD_NAME=workload-kind-template-smoke
export WORKLOAD_IMAGE=registry.k8s.io/e2e-test-images/agnhost:2.53
export TIER_C_NODE_SELECTOR_KEY=scale-promotion.nvidia.com/tier-c
export TIER_C_NODE_SELECTOR_VALUE=true
export NVB_IMAGE=example.invalid/nvbandwidth:test
export NVB_GPUS_PER_NODE=1
export NVB_NUM_RANKS=2
export NVB_REPS_PER_BENCHMARK=1
export NVB_BUFSIZE_PER_BENCHMARK_REP=64
# shellcheck disable=SC2016 # envsubst needs literal variable names.
template_vars='${TRIAL_ID} ${TRIAL_NAMESPACE} ${EXPECTED_PODS} ${COMPUTE_DOMAIN_NAME} ${CHANNEL_TEMPLATE_NAME} ${WORKLOAD_NAME} ${WORKLOAD_IMAGE} ${TIER_C_NODE_SELECTOR_KEY} ${TIER_C_NODE_SELECTOR_VALUE}'
envsubst "${template_vars}" < "${SCRIPT_DIR}/fixtures/persistent-agent-tier-c/computedomain.yaml.tmpl" > "${ARTIFACTS}/computedomain-smoke.yaml"
envsubst "${template_vars}" < "${SCRIPT_DIR}/fixtures/persistent-agent-tier-c/workload.yaml.tmpl" > "${ARTIFACTS}/workload-smoke.yaml"
# shellcheck disable=SC2016 # envsubst needs literal variable names.
nvb_template_vars='${TRIAL_ID} ${TRIAL_NAMESPACE} ${EXPECTED_PODS} ${CHANNEL_TEMPLATE_NAME} ${WORKLOAD_NAME} ${TIER_C_NODE_SELECTOR_KEY} ${TIER_C_NODE_SELECTOR_VALUE} ${NVB_IMAGE} ${NVB_GPUS_PER_NODE} ${NVB_NUM_RANKS} ${NVB_REPS_PER_BENCHMARK} ${NVB_BUFSIZE_PER_BENCHMARK_REP}'
envsubst "${nvb_template_vars}" < "${SCRIPT_DIR}/fixtures/persistent-agent-tier-c/nvbandwidth-workload.yaml.tmpl" > "${ARTIFACTS}/nvbandwidth-workload-smoke.yaml"
yq eval-all '.' "${ARTIFACTS}/computedomain-smoke.yaml" "${ARTIFACTS}/workload-smoke.yaml" "${ARTIFACTS}/nvbandwidth-workload-smoke.yaml" > /dev/null
kubectl apply --dry-run=server -f "${ARTIFACTS}/computedomain-smoke.yaml" -f "${ARTIFACTS}/workload-smoke.yaml" > "${ARTIFACTS}/fixture-server-dry-run.txt"

cat <<'EOF' | kubectl apply -f - > "${ARTIFACTS}/apply.log"
apiVersion: v1
kind: Pod
metadata:
  name: worker-0
  namespace: pa-tier-c-smoke
  labels:
    scale-promotion-smoke: "true"
spec:
  containers:
  - name: workload
    image: busybox:1.36.1
    command: [sh, -c, 'sleep 5; touch /tmp/ready; sleep 3600']
    readinessProbe:
      exec:
        command: [test, -f, /tmp/ready]
      periodSeconds: 1
---
apiVersion: v1
kind: Pod
metadata:
  name: worker-1
  namespace: pa-tier-c-smoke
  labels:
    scale-promotion-smoke: "true"
spec:
  containers:
  - name: workload
    image: busybox:1.36.1
    command: [sh, -c, 'sleep 5; touch /tmp/ready; sleep 3600']
    readinessProbe:
      exec:
        command: [test, -f, /tmp/ready]
      periodSeconds: 1
EOF

kubectl wait -n pa-tier-c-smoke --for=condition=PodScheduled pods -l scale-promotion-smoke=true --timeout=120s > /dev/null
sleep 1
t2="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl wait -n pa-tier-c-smoke --for=condition=Ready pods -l scale-promotion-smoke=true --timeout=120s > /dev/null
kubectl get pods -n pa-tier-c-smoke -l scale-promotion-smoke=true -o json > "${ARTIFACTS}/pods.json"

pod0_uid="$(jq -r '.items[] | select(.metadata.name == "worker-0") | .metadata.uid' "${ARTIFACTS}/pods.json")"
pod1_uid="$(jq -r '.items[] | select(.metadata.name == "worker-1") | .metadata.uid' "${ARTIFACTS}/pods.json")"
claim0_uid="claim-${pod0_uid}"
claim1_uid="claim-${pod1_uid}"
jq -n \
  --arg pod0 "${pod0_uid}" --arg pod1 "${pod1_uid}" \
  --arg claim0 "${claim0_uid}" --arg claim1 "${claim1_uid}" \
  '{items:[
    {metadata:{namespace:"pa-tier-c-smoke",name:"channel-0",uid:$claim0},status:{reservedFor:[{uid:$pod0}]}},
    {metadata:{namespace:"pa-tier-c-smoke",name:"channel-1",uid:$claim1},status:{reservedFor:[{uid:$pod1}]}}
  ]}' > "${ARTIFACTS}/claims.json"
{
  printf '%s Prepared devices for claim '\''pa-tier-c-smoke/channel-0:%s'\'': []\n' "${t2}" "${claim0_uid}"
  printf '%s Prepared devices for claim '\''pa-tier-c-smoke/channel-1:%s'\'': []\n' "${t2}" "${claim1_uid}"
} > "${ARTIFACTS}/kubelet-plugin.log"

cd "${REPO_ROOT}"
go run ./hack/tools/persistent-agent-timeline \
  --pods "${ARTIFACTS}/pods.json" \
  --claims "${ARTIFACTS}/claims.json" \
  --kubelet-log "${ARTIFACTS}/kubelet-plugin.log" \
  --output-dir "${ARTIFACTS}" \
  --trial-id kind-smoke \
  --provider persistent-agent-v1 \
  --shape 1x2 \
  --expected-pods 2 \
  --allow-creation-timestamp-t0

jq -e '.pods == 2 and .t0Source == "pod-creation-timestamp" and .jobMaximumMS > 0' "${ARTIFACTS}/result.json" > /dev/null
test "$(wc -l < "${ARTIFACTS}/timeline.csv" | tr -d ' ')" = "3"

if TIER_C_PROMOTION_RUN=true \
   TIER_C_SHAPE=1x2 \
   TIER_C_NODE_SELECTOR=scale-promotion-smoke=true \
   bash "${SCRIPT_DIR}/persistent-agent-tier-c-test.sh" > "${ARTIFACTS}/promotion-guard.log" 2>&1; then
  echo "ERROR: Tier C promotion guard accepted a 1x2 shape" >&2
  exit 1
fi
rg -q 'promotion run must be 1x18 or 1x144' "${ARTIFACTS}/promotion-guard.log"

echo "PASS: persistent-agent Tier C Kind smoke"
echo "Artifacts: ${ARTIFACTS}"
