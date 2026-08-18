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

# Runs a Tier C T0-T3 trial on an already-installed, schedulable cluster.
# The default fixture proves the DRA/channel control path. A promotion run must
# additionally provide a real data-plane script and apiserver audit evidence.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
FIXTURE_DIR="${SCRIPT_DIR}/fixtures/persistent-agent-tier-c"
TIER_C_SOURCE_WORKTREE="${TIER_C_SOURCE_WORKTREE:-${REPO_ROOT}}"
SHORT_SHA="$(git -C "${TIER_C_SOURCE_WORKTREE}" rev-parse --short=12 HEAD)"
HARNESS_SHA="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-${SHORT_SHA}}"
ARTIFACTS="${ARTIFACTS:-/tmp/persistent-agent-tier-c/${RUN_ID}}"

TIER_C_PROVIDER="${TIER_C_PROVIDER:-persistent-agent-v1}"
TIER_C_SHAPE="${TIER_C_SHAPE:-1x2}"
TIER_C_TRIALS="${TIER_C_TRIALS:-1}"
TIER_C_WARMUP_TRIALS="${TIER_C_WARMUP_TRIALS:-0}"
TIER_C_SCENARIO="${TIER_C_SCENARIO:-cold-domain}"
TIER_C_ARM="${TIER_C_ARM:-standalone}"
TIER_C_BLOCK="${TIER_C_BLOCK:-0}"
TIER_C_PROMOTION_RUN="${TIER_C_PROMOTION_RUN:-false}"
TIER_C_NODE_SELECTOR="${TIER_C_NODE_SELECTOR:-}"
TIER_C_DRIVER_NAMESPACE="${TIER_C_DRIVER_NAMESPACE:-nvidia-dra-driver-gpu}"
# Empty means discover Pods with the compute-domains container in the driver
# namespace. Helm's component label key changes with nameOverride, so it is not
# a stable default. Set this only to override discovery for an unusual install.
TIER_C_KUBELET_SELECTOR="${TIER_C_KUBELET_SELECTOR:-}"
TIER_C_AGENT_SELECTOR="${TIER_C_AGENT_SELECTOR:-resource.nvidia.com/persistentComputeDomainAgent=true}"
TIER_C_TIMEOUT_SECONDS="${TIER_C_TIMEOUT_SECONDS:-1800}"
TIER_C_RETIRE_TIMEOUT_SECONDS="${TIER_C_RETIRE_TIMEOUT_SECONDS:-600}"
TIER_C_T0_MODE="${TIER_C_T0_MODE:-creationTimestamp}"
TIER_C_AUDIT_LOG="${TIER_C_AUDIT_LOG:-}"
TIER_C_CLOCK_SKEW_FILE="${TIER_C_CLOCK_SKEW_FILE:-}"
TIER_C_FLEET_WARMUP_FILE="${TIER_C_FLEET_WARMUP_FILE:-}"
TIER_C_PROFILE="${TIER_C_PROFILE:-directional}"
TIER_C_CACHE_STATE="${TIER_C_CACHE_STATE:-unspecified}"
TIER_C_DATA_PLANE_SCRIPT="${TIER_C_DATA_PLANE_SCRIPT:-}"
TIER_C_DATA_PLANE_CADENCE="${TIER_C_DATA_PLANE_CADENCE:-all}"
TIER_C_SMOKE_SCRIPT="${TIER_C_SMOKE_SCRIPT:-}"
TIER_C_OBSERVABILITY_SCRIPT="${TIER_C_OBSERVABILITY_SCRIPT:-}"
TIER_C_COMPUTE_DOMAIN_TEMPLATE="${TIER_C_COMPUTE_DOMAIN_TEMPLATE:-${FIXTURE_DIR}/computedomain.yaml.tmpl}"
TIER_C_WORKLOAD_TEMPLATE="${TIER_C_WORKLOAD_TEMPLATE:-${FIXTURE_DIR}/workload.yaml.tmpl}"
TIER_C_POD_SELECTOR_TEMPLATE="${TIER_C_POD_SELECTOR_TEMPLATE:-resource.nvidia.com/scale-promotion-trial=\${TRIAL_ID}}"
TIER_C_WORKLOAD_IMAGE="${TIER_C_WORKLOAD_IMAGE:-registry.k8s.io/e2e-test-images/agnhost:2.53}"
KEEP_FAILED_FIXTURE="${KEEP_FAILED_FIXTURE:-true}"

for tool in date envsubst git go jq kubectl python3 tee yq; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done

case "${TIER_C_PROVIDER}" in
  main|legacy-v1|persistent-agent-v1) ;;
  *) echo "ERROR: TIER_C_PROVIDER must be main, legacy-v1, or persistent-agent-v1" >&2; exit 1 ;;
esac

cliques="${TIER_C_SHAPE%x*}"
members="${TIER_C_SHAPE#*x}"
if [[ ! "${cliques}" =~ ^[1-9][0-9]*$ || ! "${members}" =~ ^[1-9][0-9]*$ || "${TIER_C_SHAPE}" != "${cliques}x${members}" ]]; then
  echo "ERROR: TIER_C_SHAPE must be CLIQUESxMEMBERS" >&2
  exit 1
fi
EXPECTED_PODS=$((cliques * members))
export EXPECTED_PODS
NVB_IMAGE="${NVB_IMAGE:-ghcr.io/nvidia/k8s-samples:nvbandwidth-6dc12f17}"
NVB_GPUS_PER_NODE="${NVB_GPUS_PER_NODE:-1}"
NVB_NUM_RANKS="${NVB_NUM_RANKS:-$((EXPECTED_PODS * NVB_GPUS_PER_NODE))}"
NVB_REPS_PER_BENCHMARK="${NVB_REPS_PER_BENCHMARK:-5}"
NVB_BUFSIZE_PER_BENCHMARK_REP="${NVB_BUFSIZE_PER_BENCHMARK_REP:-1024}"
export NVB_IMAGE NVB_GPUS_PER_NODE NVB_NUM_RANKS NVB_REPS_PER_BENCHMARK NVB_BUFSIZE_PER_BENCHMARK_REP

if [[ ! "${TIER_C_TRIALS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: TIER_C_TRIALS must be positive" >&2
  exit 1
fi
if [[ ! "${TIER_C_WARMUP_TRIALS}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: TIER_C_WARMUP_TRIALS must be zero or positive" >&2
  exit 1
fi
if [[ ! "${TIER_C_BLOCK}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: TIER_C_BLOCK must be zero or positive" >&2
  exit 1
fi
case "${TIER_C_SCENARIO}" in
  cold-domain|warm-workload) ;;
  *) echo "ERROR: TIER_C_SCENARIO must be cold-domain or warm-workload" >&2; exit 1 ;;
esac
case "${TIER_C_DATA_PLANE_CADENCE}" in
  all|key|none) ;;
  *) echo "ERROR: TIER_C_DATA_PLANE_CADENCE must be all, key, or none" >&2; exit 1 ;;
esac
for hook in "${TIER_C_DATA_PLANE_SCRIPT}" "${TIER_C_SMOKE_SCRIPT}" "${TIER_C_OBSERVABILITY_SCRIPT}"; do
  if [[ -n "${hook}" && ! -x "${hook}" ]]; then
    echo "ERROR: configured hook ${hook} must be executable" >&2
    exit 1
  fi
done
if [[ -z "${TIER_C_NODE_SELECTOR}" ]]; then
  echo "ERROR: TIER_C_NODE_SELECTOR must identify exactly ${EXPECTED_PODS} capable Nodes" >&2
  exit 1
fi
if [[ ! "${TIER_C_NODE_SELECTOR}" =~ ^([a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?\.)*([A-Za-z0-9][-A-Za-z0-9_.]*/)?[A-Za-z0-9][-A-Za-z0-9_.]*=([A-Za-z0-9][-A-Za-z0-9_.]*)$ ]]; then
  echo "ERROR: TIER_C_NODE_SELECTOR must be one exact Kubernetes label key=value so the fixture can enforce placement" >&2
  exit 1
fi
TIER_C_NODE_SELECTOR_KEY="${TIER_C_NODE_SELECTOR%%=*}"
TIER_C_NODE_SELECTOR_VALUE="${TIER_C_NODE_SELECTOR#*=}"
export TIER_C_NODE_SELECTOR_KEY TIER_C_NODE_SELECTOR_VALUE
if [[ "${TIER_C_T0_MODE}" != "audit" && "${TIER_C_T0_MODE}" != "creationTimestamp" ]]; then
  echo "ERROR: TIER_C_T0_MODE must be audit or creationTimestamp" >&2
  exit 1
fi
if [[ "${TIER_C_T0_MODE}" == "audit" && -z "${TIER_C_AUDIT_LOG}" ]]; then
  echo "ERROR: audit T0 mode requires TIER_C_AUDIT_LOG" >&2
  exit 1
fi

if [[ "${TIER_C_PROMOTION_RUN}" == "true" ]]; then
  if [[ "${TIER_C_SHAPE}" != "1x18" && "${TIER_C_SHAPE}" != "1x144" ]]; then
    echo "ERROR: a Tier C promotion run must be 1x18 or 1x144" >&2
    exit 1
  fi
  if (( TIER_C_TRIALS < 30 )); then
    echo "ERROR: a Tier C promotion run requires at least 30 trials" >&2
    exit 1
  fi
  if [[ "${TIER_C_T0_MODE}" != "audit" ]]; then
    echo "ERROR: a Tier C promotion run requires audit ResponseComplete T0" >&2
    exit 1
  fi
  if [[ -z "${TIER_C_DATA_PLANE_SCRIPT}" ]]; then
    echo "ERROR: a Tier C promotion run requires TIER_C_DATA_PLANE_SCRIPT" >&2
    exit 1
  fi
  if [[ "${TIER_C_DATA_PLANE_CADENCE}" != "all" ]]; then
    echo "ERROR: a Tier C promotion run requires data-plane validation on every trial" >&2
    exit 1
  fi
  if [[ -z "${TIER_C_OBSERVABILITY_SCRIPT}" || ! -x "${TIER_C_OBSERVABILITY_SCRIPT}" ]]; then
    echo "ERROR: a Tier C promotion run requires executable TIER_C_OBSERVABILITY_SCRIPT for controller/scheduler/etcd/resource evidence" >&2
    exit 1
  fi
  if [[ -z "${TIER_C_CLOCK_SKEW_FILE}" || ! -s "${TIER_C_CLOCK_SKEW_FILE}" ]]; then
    echo "ERROR: a Tier C promotion run requires non-empty TIER_C_CLOCK_SKEW_FILE evidence" >&2
    exit 1
  fi
  if [[ "${TIER_C_CACHE_STATE}" != "warm" && "${TIER_C_CACHE_STATE}" != "cold" ]]; then
    echo "ERROR: a Tier C promotion run requires TIER_C_CACHE_STATE=warm or cold" >&2
    exit 1
  fi
  if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" && "${TIER_C_PROFILE}" != "persistent-warm" ]]; then
    echo "ERROR: persistent-agent promotion runs require TIER_C_PROFILE=persistent-warm" >&2
    exit 1
  fi
  if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" && ( -z "${TIER_C_FLEET_WARMUP_FILE}" || ! -s "${TIER_C_FLEET_WARMUP_FILE}" ) ]]; then
    echo "ERROR: persistent-agent promotion runs require non-empty TIER_C_FLEET_WARMUP_FILE evidence" >&2
    exit 1
  fi
  if [[ "${TIER_C_PROVIDER}" == "main" && "${TIER_C_PROFILE}" != "main-default" && "${TIER_C_PROFILE}" != "main-tuned" ]]; then
    echo "ERROR: main promotion runs require TIER_C_PROFILE=main-default or main-tuned" >&2
    exit 1
  fi
  if [[ "${TIER_C_PROVIDER}" == "legacy-v1" && "${TIER_C_PROFILE}" != "legacy-default" && "${TIER_C_PROFILE}" != "legacy-tuned" ]]; then
    echo "ERROR: legacy promotion runs require TIER_C_PROFILE=legacy-default or legacy-tuned" >&2
    exit 1
  fi
fi

mkdir -p "${ARTIFACTS}"
: > "${ARTIFACTS}/lifecycle.jsonl"

CURRENT_NAMESPACE=""
WATCH_PIDS=()

stop_watches() {
  local pid
  for pid in "${WATCH_PIDS[@]:-}"; do
    kill "${pid}" > /dev/null 2>&1 || true
    wait "${pid}" > /dev/null 2>&1 || true
  done
  WATCH_PIDS=()
}

now_epoch_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

now_rfc3339_ns() {
  python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))'
}

collect_failure() {
  local directory="$1"
  mkdir -p "${directory}"
  kubectl get pods,resourceclaims -A -o yaml > "${directory}/failure-cluster-objects.yaml" 2>&1 || true
  kubectl get computedomains,computedomaincliquesnapshots,computedomaincliquereservations -A -o yaml > "${directory}/failure-compute-domains.yaml" 2>&1 || true
  kubectl get events -A --sort-by=.metadata.creationTimestamp > "${directory}/failure-events.txt" 2>&1 || true
}

on_exit() {
  local rc=$?
  stop_watches
  if (( rc != 0 )); then
    collect_failure "${ARTIFACTS}"
    if [[ "${KEEP_FAILED_FIXTURE}" == "true" ]]; then
      echo "FAILED: preserving namespace ${CURRENT_NAMESPACE:-<none>} for diagnosis" >&2
    elif [[ -n "${CURRENT_NAMESPACE}" ]]; then
      kubectl delete namespace "${CURRENT_NAMESPACE}" --wait=false > /dev/null 2>&1 || true
    fi
  fi
  exit "${rc}"
}
trap on_exit EXIT

server_version="$(kubectl version -o json | jq -r '.serverVersion.gitVersion')"
if ! kubectl api-resources --api-group=resource.k8s.io -o name | rg -q '^resourceclaims\.resource\.k8s\.io$'; then
  echo "ERROR: current cluster does not serve resource.k8s.io ResourceClaims" >&2
  exit 1
fi

SELECTED_NODES=()
selected_nodes_payload="$(kubectl get nodes -l "${TIER_C_NODE_SELECTOR}" -o json)"
while IFS= read -r node; do
  SELECTED_NODES+=("${node}")
done < <(jq -r '.items[].metadata.name' <<<"${selected_nodes_payload}" | sort)
if (( ${#SELECTED_NODES[@]} != EXPECTED_PODS )); then
  echo "ERROR: selector '${TIER_C_NODE_SELECTOR}' selected ${#SELECTED_NODES[@]} Nodes, want exactly ${EXPECTED_PODS}" >&2
  exit 1
fi
printf '%s\n' "${SELECTED_NODES[@]}" > "${ARTIFACTS}/selected-nodes.txt"
if ! jq -e --argjson cliques "${cliques}" --argjson members "${members}" '
  [.items[].metadata.labels["nvidia.com/gpu.clique"] // ""] as $ids |
  all($ids[]; . != "") and
  (($ids | sort | group_by(.)) as $groups |
   ($groups | length) == $cliques and all($groups[]; length == $members))
' <<<"${selected_nodes_payload}" > /dev/null; then
  echo "ERROR: selected Nodes do not form exactly ${cliques} non-empty physical cliques of ${members} members" >&2
  exit 1
fi

agent_payload="$(kubectl get pods -n "${TIER_C_DRIVER_NAMESPACE}" -l "${TIER_C_AGENT_SELECTOR}" -o json 2>/dev/null || printf '{"items":[]}')"
agent_count="$(jq '.items | length' <<<"${agent_payload}")"
kubelet_pods_json() {
  if [[ -n "${TIER_C_KUBELET_SELECTOR}" ]]; then
    kubectl get pods -A -l "${TIER_C_KUBELET_SELECTOR}" -o json
  else
    kubectl get pods -n "${TIER_C_DRIVER_NAMESPACE}" -o json
  fi | jq -f "${FIXTURE_DIR}/kubelet-pods.jq"
}

kubelet_payload="$(kubelet_pods_json)"
printf '%s\n' "${kubelet_payload}" > "${ARTIFACTS}/kubelet-pods-before.json"
printf '%s\n' "${agent_payload}" > "${ARTIFACTS}/agent-pods-before.json"
for node in "${SELECTED_NODES[@]}"; do
  kubelet_on_node="$(jq --arg node "${node}" '[.items[] | select(.spec.nodeName == $node and any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"${kubelet_payload}")"
  if (( kubelet_on_node != 1 )); then
    echo "ERROR: Node ${node} has ${kubelet_on_node} Ready compute-domain kubelet-plugin Pods, want exactly one" >&2
    exit 1
  fi
done
if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" ]]; then
  for node in "${SELECTED_NODES[@]}"; do
    agent_on_node="$(jq --arg node "${node}" '[.items[] | select(.spec.nodeName == $node and any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"${agent_payload}")"
    if (( agent_on_node != 1 )); then
      echo "ERROR: Node ${node} has ${agent_on_node} Ready persistent-agent Pods, want exactly one" >&2
      exit 1
    fi
    node_payload="$(kubectl get node "${node}" -o json)"
    clique_id="$(jq -r '.metadata.labels["nvidia.com/gpu.clique"] // ""' <<<"${node_payload}")"
    startup_id="$(jq -r '.metadata.annotations["resource.nvidia.com/computeDomainCliqueStartupID"] // ""' <<<"${node_payload}")"
    route="$(jq -r '.metadata.labels["resource.nvidia.com/computeDomain"] // ""' <<<"${node_payload}")"
    attestation="$(jq -r '.metadata.annotations["resource.nvidia.com/computeDomainAttestation"] // ""' <<<"${node_payload}")"
    if [[ -z "${clique_id}" || "${startup_id}" != "${clique_id}" || -n "${route}" || -n "${attestation}" ]]; then
      echo "ERROR: Node ${node} is not a clean, topology-verified persistent-agent candidate" >&2
      exit 1
    fi
  done
else
  if (( agent_count != 0 )); then
    echo "ERROR: non-persistent comparison requires full persistent-agent fleet decommission, not a gate toggle" >&2
    exit 1
  fi
fi

{
  echo "run_id=${RUN_ID}"
  echo "commit=$(git -C "${TIER_C_SOURCE_WORKTREE}" rev-parse HEAD)"
  echo "branch=$(git -C "${TIER_C_SOURCE_WORKTREE}" branch --show-current)"
  echo "dirty_files=$(git -C "${TIER_C_SOURCE_WORKTREE}" status --porcelain=v1 | wc -l | tr -d ' ')"
  echo "harness_commit=${HARNESS_SHA}"
  echo "server_version=${server_version}"
  echo "provider=${TIER_C_PROVIDER}"
  echo "arm=${TIER_C_ARM}"
  echo "block=${TIER_C_BLOCK}"
  echo "scenario=${TIER_C_SCENARIO}"
  echo "shape=${TIER_C_SHAPE}"
  echo "trials=${TIER_C_TRIALS}"
  echo "warmup_trials=${TIER_C_WARMUP_TRIALS}"
  echo "promotion_run=${TIER_C_PROMOTION_RUN}"
  echo "t0_mode=${TIER_C_T0_MODE}"
  echo "profile=${TIER_C_PROFILE}"
  echo "cache_state=${TIER_C_CACHE_STATE}"
  echo "node_selector=${TIER_C_NODE_SELECTOR}"
  echo "kubelet_selector=${TIER_C_KUBELET_SELECTOR:-auto:compute-domains-container}"
  echo "workload_template=${TIER_C_WORKLOAD_TEMPLATE}"
  echo "workload_image=${TIER_C_WORKLOAD_IMAGE}"
  echo "nvbandwidth_image=${NVB_IMAGE}"
  echo "nvbandwidth_gpus_per_node=${NVB_GPUS_PER_NODE}"
  echo "data_plane_script=${TIER_C_DATA_PLANE_SCRIPT:-none}"
  echo "data_plane_cadence=${TIER_C_DATA_PLANE_CADENCE}"
  echo "smoke_script=${TIER_C_SMOKE_SCRIPT:-none}"
  echo "observability_script=${TIER_C_OBSERVABILITY_SCRIPT:-none}"
} > "${ARTIFACTS}/source.txt"
if [[ -n "${TIER_C_CLOCK_SKEW_FILE}" ]]; then
  cp "${TIER_C_CLOCK_SKEW_FILE}" "${ARTIFACTS}/clock-skew.txt"
fi
if [[ -n "${TIER_C_FLEET_WARMUP_FILE}" ]]; then
  cp "${TIER_C_FLEET_WARMUP_FILE}" "${ARTIFACTS}/fleet-warmup.txt"
fi
git -C "${TIER_C_SOURCE_WORKTREE}" log -1 --show-signature --format=fuller > "${ARTIFACTS}/signature.txt" 2>&1
kubectl version -o yaml > "${ARTIFACTS}/versions.yaml"
kubectl get nodes -o wide > "${ARTIFACTS}/nodes-before.txt"
kubectl get nodes -o yaml > "${ARTIFACTS}/nodes-before.yaml"

capture_metrics() {
  local path="$1"
  kubectl get --raw /metrics > "${path}" 2> "${path}.error" || true
}

render_template() {
  local source="$1"
  local destination="$2"
  # shellcheck disable=SC2016 # envsubst needs literal variable names.
  envsubst '${TRIAL_ID} ${TRIAL_NAMESPACE} ${EXPECTED_PODS} ${COMPUTE_DOMAIN_NAME} ${CHANNEL_TEMPLATE_NAME} ${WORKLOAD_NAME} ${WORKLOAD_IMAGE} ${TIER_C_NODE_SELECTOR_KEY} ${TIER_C_NODE_SELECTOR_VALUE} ${NVB_IMAGE} ${NVB_GPUS_PER_NODE} ${NVB_NUM_RANKS} ${NVB_REPS_PER_BENCHMARK} ${NVB_BUFSIZE_PER_BENCHMARK_REP}' < "${source}" > "${destination}"
}

wait_for_ready_pods() {
  local namespace="$1"
  local selector="$2"
  local deadline=$((SECONDS + TIER_C_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    local payload count ready failed
    payload="$(kubectl get pods -n "${namespace}" -l "${selector}" -o json)"
    count="$(jq '.items | length' <<<"${payload}")"
    ready="$(jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"${payload}")"
    failed="$(jq '[.items[] | select(.status.phase == "Failed")] | length' <<<"${payload}")"
    if (( failed > 0 )); then
      echo "ERROR: ${failed} workload Pods failed" >&2
      return 1
    fi
    if (( count > EXPECTED_PODS )); then
      echo "ERROR: observed ${count} workload Pods, want exactly ${EXPECTED_PODS}" >&2
      return 1
    fi
    if (( count == EXPECTED_PODS && ready == EXPECTED_PODS )); then
      return 0
    fi
    sleep 2
  done
  echo "ERROR: timed out waiting for ${EXPECTED_PODS} Ready workload Pods" >&2
  return 1
}

collect_kubelet_logs() {
  local since_time="$1"
  local destination="$2"
  : > "${destination}"
  local pod namespace
  while IFS=$'\t' read -r namespace pod; do
    kubectl logs -n "${namespace}" "${pod}" -c compute-domains --timestamps --since-time="${since_time}" >> "${destination}" 2>/dev/null || true
    kubectl logs -n "${namespace}" "${pod}" -c compute-domains --previous --timestamps --since-time="${since_time}" >> "${destination}" 2>/dev/null || true
  done < <(kubelet_pods_json | jq -r '.items[] | [.metadata.namespace,.metadata.name] | @tsv')
}

collect_driver_logs() {
  local since_time="$1"
  local destination="$2"
  mkdir -p "${destination}"
  local pod container
  while IFS=$'\t' read -r pod container; do
    kubectl logs -n "${TIER_C_DRIVER_NAMESPACE}" "${pod}" -c "${container}" \
      --timestamps --since-time="${since_time}" \
      > "${destination}/${pod}-${container}.log" 2>&1 || true
    kubectl logs -n "${TIER_C_DRIVER_NAMESPACE}" "${pod}" -c "${container}" \
      --previous --timestamps --since-time="${since_time}" \
      > "${destination}/${pod}-${container}.previous.log" 2>&1 || true
  done < <(kubectl get pods -n "${TIER_C_DRIVER_NAMESPACE}" -o json | jq -r '.items[] | .metadata.name as $pod | .spec.containers[].name | [$pod,.] | @tsv')
}

capture_resource_usage() {
  local directory="$1"
  kubectl top nodes > "${directory}/node-usage.txt" 2> "${directory}/node-usage.error" || true
  kubectl top pods -A --containers > "${directory}/pod-usage.txt" 2> "${directory}/pod-usage.error" || true
}

run_observability_hook() {
  local phase="$1"
  local directory="$2"
  if [[ -z "${TIER_C_OBSERVABILITY_SCRIPT}" ]]; then
    return 0
  fi
  TIER_C_OBSERVABILITY_PHASE="${phase}" \
  TIER_C_TRIAL_NAMESPACE="${TRIAL_NAMESPACE}" \
  TIER_C_TRIAL_ID="${TRIAL_ID}" \
  TIER_C_TRIAL_ARTIFACTS="${directory}" \
    "${TIER_C_OBSERVABILITY_SCRIPT}" \
      > "${directory}/observability-${phase}.log" 2>&1
}

run_trial_hook() {
  local script="$1"
  local log_name="$2"
  local directory="$3"
  if [[ -z "${script}" ]]; then
    printf '%s\n' "not-run: no ${log_name} hook supplied" > "${directory}/${log_name}.log"
    return 0
  fi
  TIER_C_TRIAL_NAMESPACE="${TRIAL_NAMESPACE}" \
  TIER_C_TRIAL_ID="${TRIAL_ID}" \
  TIER_C_COMPUTE_DOMAIN_NAME="${COMPUTE_DOMAIN_NAME}" \
  TIER_C_WORKLOAD_NAME="${WORKLOAD_NAME}" \
  TIER_C_POD_SELECTOR="${pod_selector}" \
  TIER_C_TRIAL_ARTIFACTS="${directory}" \
    "${script}" 2>&1 | tee "${directory}/${log_name}.log"
}

run_data_plane_for_cycle() {
  local measured_index="$1"
  case "${TIER_C_DATA_PLANE_CADENCE}" in
    all) return 0 ;;
    none) return 1 ;;
    key)
      (( measured_index > 0 )) || return 1
      (( measured_index == 1 || measured_index == (TIER_C_TRIALS + 1) / 2 || measured_index == TIER_C_TRIALS ))
      ;;
  esac
}

start_trial_watches() {
  local directory="$1"
  local namespace="$2"
  local selector="$3"
  local cd_uid="$4"

  kubectl get pods -n "${namespace}" -l "${selector}" --watch --output-watch-events -o json > "${directory}/pod-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get resourceclaims -n "${namespace}" --watch --output-watch-events -o json > "${directory}/claim-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get computedomains -n "${namespace}" --watch --output-watch-events -o json > "${directory}/computedomain-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get daemonsets -A -l "resource.nvidia.com/computeDomain=${cd_uid}" --watch --output-watch-events -o json > "${directory}/daemonset-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get pods -A -l "resource.nvidia.com/computeDomain=${cd_uid}" --watch --output-watch-events -o json > "${directory}/driver-pod-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get resourceclaimtemplates -A -l "resource.nvidia.com/computeDomain=${cd_uid}" --watch --output-watch-events -o json > "${directory}/template-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get computedomaincliquesnapshots -n "${TIER_C_DRIVER_NAMESPACE}" -l "resource.nvidia.com/computeDomain=${cd_uid}" --watch --output-watch-events -o json > "${directory}/snapshot-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get computedomaincliquereservations -l "resource.nvidia.com/computeDomain=${cd_uid}" --watch --output-watch-events -o json > "${directory}/reservation-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get computedomaincliqueretirementevidences -n "${TIER_C_DRIVER_NAMESPACE}" -l "resource.nvidia.com/computeDomain=${cd_uid}" --watch --output-watch-events -o json > "${directory}/retirement-evidence-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
  kubectl get nodes -l "${TIER_C_NODE_SELECTOR}" --watch --output-watch-events -o json > "${directory}/node-watch.json" 2>&1 &
  WATCH_PIDS+=("$!")
}

capture_object_inventory() {
  local directory="$1"
  local phase="$2"
  local namespace="$3"
  local cd_uid="$4"
  local destination="${directory}/inventory-${phase}.json"

  jq -n \
    --argjson workloadPods "$(kubectl get pods -n "${namespace}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    --argjson workloadClaims "$(kubectl get resourceclaims -n "${namespace}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    --argjson driverPods "$(kubectl get pods -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    --argjson daemonSets "$(kubectl get daemonsets -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    --argjson templates "$(kubectl get resourceclaimtemplates -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    --argjson snapshots "$(kubectl get computedomaincliquesnapshots -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    --argjson reservations "$(kubectl get computedomaincliquereservations -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 0)" \
    '{workloadPods:$workloadPods,workloadClaims:$workloadClaims,driverPods:$driverPods,daemonSets:$daemonSets,templates:$templates,snapshots:$snapshots,reservations:$reservations}' \
    > "${destination}"
}

cleanup_workload() {
  local directory="$1"
  local namespace="$2"
  local workload_manifest="$3"
  local cleanup_start cleanup_done deadline

  cleanup_start="$(now_epoch_ms)"
  kubectl delete -f "${workload_manifest}" --ignore-not-found --wait=false > "${directory}/cleanup-workload.log" 2>&1 || true
  deadline=$((SECONDS + TIER_C_RETIRE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    local pods claims
    pods="$(kubectl get pods -n "${namespace}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    claims="$(kubectl get resourceclaims -n "${namespace}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    if [[ "${pods}" == "0" && "${claims}" == "0" ]]; then
      cleanup_done="$(now_epoch_ms)"
      jq -n \
        --argjson requestedAtMS "${cleanup_start}" \
        --argjson completeAtMS "${cleanup_done}" \
        '{requestedAtMS:$requestedAtMS,completeAtMS:$completeAtMS,durationMS:($completeAtMS-$requestedAtMS)}' \
        > "${directory}/workload-cleanup.json"
      return 0
    fi
    sleep 0.25
  done
  echo "ERROR: workload Pods or claims did not disappear in ${namespace}" >&2
  return 1
}

retire_domain() {
  local directory="$1"
  local namespace="$2"
  local compute_domain_name="$3"
  local cd_uid="$4"
  local workload_manifest="$5"
  local delete_namespace="$6"
  local deadline d0_ms d1_ms=0 d2_ms=0 d3_ms=0 d4_ms=0
  local d0_time d1_time="" d2_time="" d3_time="" d4_time="" d2_source=""

  kubectl delete -f "${workload_manifest}" --ignore-not-found --wait=false > "${directory}/cleanup.log" 2>&1 || true
  kubectl delete computedomain -n "${namespace}" "${compute_domain_name}" --ignore-not-found --wait=false >> "${directory}/cleanup.log" 2>&1
  d0_ms="$(now_epoch_ms)"
  d0_time="$(now_rfc3339_ns)"

  deadline=$((SECONDS + TIER_C_RETIRE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    local pods claims cds routes isolation daemonsets daemonpods templates snapshots reservations evidence
    pods="$(kubectl get pods -n "${namespace}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    claims="$(kubectl get resourceclaims -n "${namespace}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    if (( d1_ms == 0 )) && [[ "${pods}" == "0" && "${claims}" == "0" ]]; then
      d1_ms="$(now_epoch_ms)"
      d1_time="$(now_rfc3339_ns)"
    fi

    if (( d2_ms == 0 )); then
      if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" ]]; then
        snapshots="$(kubectl get computedomaincliquesnapshots -n "${TIER_C_DRIVER_NAMESPACE}" -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json)"
        reservations="$(kubectl get computedomaincliquereservations -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json)"
        if jq -e '.items | length > 0 and all(.[]; .status.phase == "Fenced")' <<<"${snapshots}" > /dev/null; then
          d2_ms="$(now_epoch_ms)"
          d2_time="$(now_rfc3339_ns)"
          d2_source="snapshot-fenced"
        elif jq -e '.items | length > 0 and all(.[]; .status.phase == "Released")' <<<"${reservations}" > /dev/null; then
          d2_ms="$(now_epoch_ms)"
          d2_time="$(now_rfc3339_ns)"
          d2_source="reservation-released"
        fi
      else
        daemonsets="$(kubectl get daemonsets -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
        daemonpods="$(kubectl get pods -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
        if [[ "${daemonsets}" == "0" && "${daemonpods}" == "0" ]]; then
          d2_ms="$(now_epoch_ms)"
          d2_time="$(now_rfc3339_ns)"
          d2_source="per-domain-daemon-gone"
        fi
      fi
    fi

    if kubectl get computedomain -n "${namespace}" "${compute_domain_name}" -o name > "${directory}/retirement-computedomain.txt" 2> "${directory}/retirement-computedomain.error"; then
      cds=1
    elif rg -qi 'notfound|not found' "${directory}/retirement-computedomain.error"; then
      cds=0
    else
      cat "${directory}/retirement-computedomain.error" >&2
      return 1
    fi
    if (( d3_ms == 0 )) && [[ "${cds}" == "0" ]]; then
      d3_ms="$(now_epoch_ms)"
      d3_time="$(now_rfc3339_ns)"
      if (( d2_ms == 0 )); then
        # The controller cannot remove its finalizer before its provider-specific
        # fence/delete contract completes. Keep the timing conservative when a
        # short-lived Fenced/Released object fell between polling observations.
        d2_ms="${d3_ms}"
        d2_time="${d3_time}"
        d2_source="compute-domain-finalizer-complete"
      fi
    fi

    routes="$(kubectl get nodes -o json | jq --arg uid "${cd_uid}" '[.items[] | select(.metadata.labels["resource.nvidia.com/computeDomain"] == $uid)] | length')"
    isolation="$(kubectl get nodes -o json | jq --arg uid "${cd_uid}" '[.items[] | select(.metadata.labels["resource.nvidia.com/persistentAgentComputeDomain"] == $uid)] | length')"
    daemonsets="$(kubectl get daemonsets -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    daemonpods="$(kubectl get pods -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    templates="$(kubectl get resourceclaimtemplates -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json 2>/dev/null | jq '.items | length' 2>/dev/null || echo 1)"
    if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" ]]; then
      snapshots="$(kubectl get computedomaincliquesnapshots -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json | jq '.items | length')"
      reservations="$(kubectl get computedomaincliquereservations -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json | jq '.items | length')"
      evidence="$(kubectl get computedomaincliqueretirementevidences -A -l "resource.nvidia.com/computeDomain=${cd_uid}" -o json | jq '.items | length')"
    else
      snapshots=0
      reservations=0
      evidence=0
    fi
    if (( d3_ms != 0 )) && [[ "${routes}" == "0" && "${isolation}" == "0" && "${daemonsets}" == "0" && "${daemonpods}" == "0" && "${templates}" == "0" && "${snapshots}" == "0" && "${reservations}" == "0" && "${evidence}" == "0" ]]; then
      d4_ms="$(now_epoch_ms)"
      d4_time="$(now_rfc3339_ns)"
      break
    fi
    sleep 0.25
  done

  if (( d1_ms == 0 || d2_ms == 0 || d3_ms == 0 || d4_ms == 0 )); then
    echo "ERROR: retirement did not reach D0-D4 in ${namespace}: D1=${d1_ms} D2=${d2_ms} D3=${d3_ms} D4=${d4_ms}" >&2
    return 1
  fi

  jq -n \
    --arg trialID "${TRIAL_ID}" --arg arm "${TIER_C_ARM}" --arg provider "${TIER_C_PROVIDER}" \
    --arg scenario "${TIER_C_SCENARIO}" --arg cycleClass "${CYCLE_CLASS:-measured}" \
    --argjson cycle "${CYCLE_NUMBER:-0}" --argjson block "${TIER_C_BLOCK}" \
    --arg d0 "${d0_time}" --arg d1 "${d1_time}" --arg d2 "${d2_time}" --arg d3 "${d3_time}" --arg d4 "${d4_time}" \
    --arg d2Source "${d2_source}" \
    --argjson d0MS "${d0_ms}" --argjson d1MS "${d1_ms}" --argjson d2MS "${d2_ms}" --argjson d3MS "${d3_ms}" --argjson d4MS "${d4_ms}" \
    '{trialID:$trialID,arm:$arm,provider:$provider,scenario:$scenario,cycleClass:$cycleClass,cycle:$cycle,block:$block,d2Source:$d2Source,
      d0:$d0,d1:$d1,d2:$d2,d3:$d3,d4:$d4,
      workloadGoneMS:($d1MS-$d0MS),fenceMS:($d2MS-$d0MS),finalizationMS:($d3MS-$d0MS),reuseReadyMS:($d4MS-$d0MS)}' \
    > "${directory}/lifecycle.json"
  jq -c . "${directory}/lifecycle.json" >> "${ARTIFACTS}/lifecycle.jsonl"

  if [[ "${delete_namespace}" == "true" ]]; then
    kubectl delete namespace "${namespace}" --wait=true --timeout="${TIER_C_RETIRE_TIMEOUT_SECONDS}s" >> "${directory}/cleanup.log" 2>&1
  fi
}

result_paths=()
provider_slug="${TIER_C_PROVIDER%-v1}"
arm_slug="$(tr '[:upper:]' '[:lower:]' <<<"${TIER_C_ARM}" | tr -cd 'a-z0-9-')"
scenario_slug="${TIER_C_SCENARIO//-}"
session_id="pa-${arm_slug:-x}-b${TIER_C_BLOCK}-${provider_slug}-${scenario_slug}-${SHORT_SHA}-$$"
session_id="${session_id:0:52}"
base_dir="${ARTIFACTS}/${TIER_C_PROVIDER}/${TIER_C_SHAPE}"
TOTAL_TRIALS=$((TIER_C_WARMUP_TRIALS + TIER_C_TRIALS))
shared_cd_uid=""
shared_workload_manifest=""

if [[ "${TIER_C_SCENARIO}" == "warm-workload" ]]; then
  TRIAL_ID="${session_id}"
  TRIAL_NAMESPACE="${session_id}"
  COMPUTE_DOMAIN_NAME="cd-${session_id}"
  COMPUTE_DOMAIN_NAME="${COMPUTE_DOMAIN_NAME:0:63}"
  CHANNEL_TEMPLATE_NAME="channel-${session_id}"
  CHANNEL_TEMPLATE_NAME="${CHANNEL_TEMPLATE_NAME:0:63}"
  WORKLOAD_NAME="setup"
  WORKLOAD_IMAGE="${TIER_C_WORKLOAD_IMAGE}"
  export TRIAL_ID TRIAL_NAMESPACE COMPUTE_DOMAIN_NAME CHANNEL_TEMPLATE_NAME WORKLOAD_NAME WORKLOAD_IMAGE
  setup_dir="${base_dir}/setup"
  mkdir -p "${setup_dir}"
  CURRENT_NAMESPACE="${TRIAL_NAMESPACE}"
  render_template "${TIER_C_COMPUTE_DOMAIN_TEMPLATE}" "${setup_dir}/computedomain.yaml"
  yq eval-all '.' "${setup_dir}/computedomain.yaml" > /dev/null
  kubectl create namespace "${TRIAL_NAMESPACE}" > "${setup_dir}/namespace-create.txt"
  kubectl apply -f "${setup_dir}/computedomain.yaml" > "${setup_dir}/apply-computedomain.txt"
  shared_cd_uid="$(kubectl get computedomain -n "${TRIAL_NAMESPACE}" "${COMPUTE_DOMAIN_NAME}" -o jsonpath='{.metadata.uid}')"
  if [[ -z "${shared_cd_uid}" ]]; then
    echo "ERROR: warm-workload ComputeDomain UID is empty" >&2
    exit 1
  fi
  printf '%s\n' "${shared_cd_uid}" > "${setup_dir}/computedomain-uid.txt"
  if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" ]]; then
    for node in "${SELECTED_NODES[@]}"; do
      kubectl label node "${node}" "resource.nvidia.com/persistentAgentComputeDomain=${shared_cd_uid}" --overwrite > /dev/null
    done
  fi
fi

for ((cycle = 1; cycle <= TOTAL_TRIALS; cycle++)); do
  if (( cycle <= TIER_C_WARMUP_TRIALS )); then
    CYCLE_CLASS="warmup"
    CYCLE_NUMBER="${cycle}"
    measured_index=0
  else
    CYCLE_CLASS="measured"
    CYCLE_NUMBER=$((cycle - TIER_C_WARMUP_TRIALS))
    measured_index="${CYCLE_NUMBER}"
  fi
  cycle_id="$(printf '%03d' "${CYCLE_NUMBER}")"
  TRIAL_ID="${session_id}-${CYCLE_CLASS:0:1}${cycle_id}"
  TRIAL_ID="${TRIAL_ID:0:63}"
  if [[ "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
    TRIAL_NAMESPACE="${TRIAL_ID}"
    COMPUTE_DOMAIN_NAME="cd-${TRIAL_ID}"
    COMPUTE_DOMAIN_NAME="${COMPUTE_DOMAIN_NAME:0:63}"
    CHANNEL_TEMPLATE_NAME="channel-${TRIAL_ID}"
    CHANNEL_TEMPLATE_NAME="${CHANNEL_TEMPLATE_NAME:0:63}"
  fi
  WORKLOAD_NAME="workload-${TRIAL_ID}"
  WORKLOAD_NAME="${WORKLOAD_NAME:0:63}"
  WORKLOAD_IMAGE="${TIER_C_WORKLOAD_IMAGE}"
  export TRIAL_ID TRIAL_NAMESPACE COMPUTE_DOMAIN_NAME CHANNEL_TEMPLATE_NAME WORKLOAD_NAME WORKLOAD_IMAGE CYCLE_CLASS CYCLE_NUMBER
  # shellcheck disable=SC2016 # envsubst needs the literal variable name.
  pod_selector="$(envsubst '${TRIAL_ID}' <<<"${TIER_C_POD_SELECTOR_TEMPLATE}")"
  trial_dir="${base_dir}/${CYCLE_CLASS}/${cycle_id}"
  if (( TIER_C_WARMUP_TRIALS == 0 )) && [[ "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
    trial_dir="${base_dir}/${cycle_id}"
  fi
  mkdir -p "${trial_dir}"
  CURRENT_NAMESPACE="${TRIAL_NAMESPACE}"

  render_template "${TIER_C_WORKLOAD_TEMPLATE}" "${trial_dir}/workload.yaml"
  yq eval-all '.' "${trial_dir}/workload.yaml" > /dev/null
  if [[ "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
    render_template "${TIER_C_COMPUTE_DOMAIN_TEMPLATE}" "${trial_dir}/computedomain.yaml"
    yq eval-all '.' "${trial_dir}/computedomain.yaml" > /dev/null
    kubectl create namespace "${TRIAL_NAMESPACE}" > "${trial_dir}/namespace-create.txt"
    kubectl apply -f "${trial_dir}/computedomain.yaml" > "${trial_dir}/apply-computedomain.txt"
    cd_uid="$(kubectl get computedomain -n "${TRIAL_NAMESPACE}" "${COMPUTE_DOMAIN_NAME}" -o jsonpath='{.metadata.uid}')"
    if [[ -z "${cd_uid}" ]]; then
      echo "ERROR: ComputeDomain UID is empty" >&2
      exit 1
    fi
    if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" ]]; then
      for node in "${SELECTED_NODES[@]}"; do
        kubectl label node "${node}" "resource.nvidia.com/persistentAgentComputeDomain=${cd_uid}" --overwrite > /dev/null
      done
    fi
  else
    cp "${setup_dir}/computedomain.yaml" "${trial_dir}/computedomain.yaml"
    cd_uid="${shared_cd_uid}"
  fi
  printf '%s\n' "${cd_uid}" > "${trial_dir}/computedomain-uid.txt"

  capture_metrics "${trial_dir}/apiserver-before.prom"
  capture_object_inventory "${trial_dir}" before "${TRIAL_NAMESPACE}" "${cd_uid}"
  run_observability_hook before "${trial_dir}"
  start_trial_watches "${trial_dir}" "${TRIAL_NAMESPACE}" "${pod_selector}" "${cd_uid}"

  trial_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  kubectl apply -f "${trial_dir}/workload.yaml" > "${trial_dir}/apply-workload.txt"
  wait_for_ready_pods "${TRIAL_NAMESPACE}" "${pod_selector}"

  kubectl get pods -n "${TRIAL_NAMESPACE}" -l "${pod_selector}" -o json > "${trial_dir}/pods.json"
  kubectl get resourceclaims -n "${TRIAL_NAMESPACE}" -o json > "${trial_dir}/claims.json"
  kubectl get computedomain -n "${TRIAL_NAMESPACE}" "${COMPUTE_DOMAIN_NAME}" -o yaml > "${trial_dir}/computedomain.yaml.result"
  kubectl get computedomaincliquesnapshots -n "${TIER_C_DRIVER_NAMESPACE}" -o json > "${trial_dir}/snapshots.json" 2>/dev/null || printf '{"items":[]}' > "${trial_dir}/snapshots.json"
  kubectl get computedomaincliquereservations -o json > "${trial_dir}/reservations.json" 2>/dev/null || printf '{"items":[]}' > "${trial_dir}/reservations.json"
  kubectl get nodes -o yaml > "${trial_dir}/nodes.yaml"
  kubectl get events -n "${TRIAL_NAMESPACE}" --sort-by=.metadata.creationTimestamp -o yaml > "${trial_dir}/events.yaml"
  collect_kubelet_logs "${trial_start}" "${trial_dir}/kubelet-plugin.log"
  collect_driver_logs "${trial_start}" "${trial_dir}/driver-logs"
  capture_resource_usage "${trial_dir}"
  capture_metrics "${trial_dir}/apiserver-after.prom"
  run_observability_hook after "${trial_dir}"

  scheduled_nodes=()
  while IFS= read -r node; do
    scheduled_nodes+=("${node}")
  done < <(jq -r '.items[].spec.nodeName' "${trial_dir}/pods.json" | sort -u)
  if (( ${#scheduled_nodes[@]} != EXPECTED_PODS )); then
    echo "ERROR: workload used ${#scheduled_nodes[@]} unique Nodes, want exactly ${EXPECTED_PODS}" >&2
    exit 1
  fi
  for node in "${scheduled_nodes[@]}"; do
    if ! printf '%s\n' "${SELECTED_NODES[@]}" | rg -qx --fixed-strings "${node}"; then
      echo "ERROR: workload scheduled on Node ${node}, outside TIER_C_NODE_SELECTOR" >&2
      exit 1
    fi
  done

  observed_provider="$(yq eval '.metadata.annotations."resource.nvidia.com/computeDomainCliqueProtocol" // ""' "${trial_dir}/computedomain.yaml.result")"
  observed_status="$(yq eval '.status.status // ""' "${trial_dir}/computedomain.yaml.result")"
  expected_provider="${TIER_C_PROVIDER}"
  if [[ "${TIER_C_PROVIDER}" == "main" ]]; then
    expected_provider=""
  fi
  if [[ "${observed_provider}" != "${expected_provider}" || "${observed_status}" != "Ready" ]]; then
    echo "ERROR: ComputeDomain protocol/status is ${observed_provider:-<absent>}/${observed_status}, want ${expected_provider:-<absent>}/Ready for subject ${TIER_C_PROVIDER}" >&2
    exit 1
  fi
  if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" ]]; then
    snapshot_count="$(jq --arg uid "${cd_uid}" '[.items[] | select(.spec.computeDomainUID == $uid and .status.phase == "Active")] | length' "${trial_dir}/snapshots.json")"
    invalid_snapshot_count="$(jq --arg uid "${cd_uid}" --argjson members "${members}" -f "${FIXTURE_DIR}/validate-active-snapshots.jq" "${trial_dir}/snapshots.json")"
    reservation_count="$(jq --arg uid "${cd_uid}" '[.items[] | select(.spec.computeDomainUID == $uid and .status.phase == "Active")] | length' "${trial_dir}/reservations.json")"
    if (( snapshot_count != cliques || invalid_snapshot_count != 0 || reservation_count != cliques )); then
      echo "ERROR: observed ${snapshot_count} Active snapshots, ${invalid_snapshot_count} malformed snapshots, and ${reservation_count} Active reservations; want ${cliques}/0/${cliques}" >&2
      exit 1
    fi
  fi

  analyzer_args=(
    --pods "${trial_dir}/pods.json"
    --claims "${trial_dir}/claims.json"
    --kubelet-log "${trial_dir}/kubelet-plugin.log"
    --output-dir "${trial_dir}"
    --trial-id "${TRIAL_ID}"
    --provider "${TIER_C_PROVIDER}"
    --shape "${TIER_C_SHAPE}"
    --expected-pods "${EXPECTED_PODS}"
  )
  if [[ "${TIER_C_T0_MODE}" == "audit" ]]; then
    cp "${TIER_C_AUDIT_LOG}" "${trial_dir}/audit.jsonl"
    analyzer_args+=(--audit-log "${trial_dir}/audit.jsonl")
  else
    analyzer_args+=(--allow-creation-timestamp-t0)
  fi
  go run ./hack/tools/persistent-agent-timeline "${analyzer_args[@]}"

  run_trial_hook "${TIER_C_SMOKE_SCRIPT}" smoke "${trial_dir}"
  if run_data_plane_for_cycle "${measured_index}"; then
    run_trial_hook "${TIER_C_DATA_PLANE_SCRIPT}" data-plane "${trial_dir}"
  else
    printf '%s\n' "not-run: cadence=${TIER_C_DATA_PLANE_CADENCE}" > "${trial_dir}/data-plane.log"
  fi

  if [[ "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
    retire_domain "${trial_dir}" "${TRIAL_NAMESPACE}" "${COMPUTE_DOMAIN_NAME}" "${cd_uid}" "${trial_dir}/workload.yaml" true
  else
    cleanup_workload "${trial_dir}" "${TRIAL_NAMESPACE}" "${trial_dir}/workload.yaml"
  fi
  capture_object_inventory "${trial_dir}" after "${TRIAL_NAMESPACE}" "${cd_uid}"
  stop_watches
  if [[ "${CYCLE_CLASS}" == "measured" ]]; then
    result_paths+=("${trial_dir}/result.json")
  fi
  if [[ "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
    CURRENT_NAMESPACE=""
  fi
done

if [[ "${TIER_C_SCENARIO}" == "warm-workload" ]]; then
  retirement_dir="${base_dir}/retirement"
  mkdir -p "${retirement_dir}"
  TRIAL_ID="${session_id}-retirement"
  CYCLE_CLASS="retirement"
  CYCLE_NUMBER=0
  retire_domain "${retirement_dir}" "${TRIAL_NAMESPACE}" "${COMPUTE_DOMAIN_NAME}" "${shared_cd_uid}" "${shared_workload_manifest:-${trial_dir}/workload.yaml}" true
  CURRENT_NAMESPACE=""
fi

if (( ${#result_paths[@]} == 0 )); then
  echo "ERROR: no measured trial results were produced" >&2
  exit 1
fi
joined_results="$(IFS=,; echo "${result_paths[*]}")"
go run ./hack/tools/persistent-agent-timeline \
  --results "${joined_results}" \
  --output-dir "${base_dir}/aggregate"

kubelet_after="$(kubelet_pods_json)"
agent_after="$(kubectl get pods -n "${TIER_C_DRIVER_NAMESPACE}" -l "${TIER_C_AGENT_SELECTOR}" -o json 2>/dev/null || printf '{"items":[]}')"
printf '%s\n' "${kubelet_after}" > "${ARTIFACTS}/kubelet-pods-after.json"
printf '%s\n' "${agent_after}" > "${ARTIFACTS}/agent-pods-after.json"
identity_filter='[.items[] | {uid:.metadata.uid,node:.spec.nodeName,restarts:([.status.containerStatuses[]?.restartCount] | add // 0)}] | sort_by(.node,.uid)'
if [[ "$(jq -c "${identity_filter}" <<<"${kubelet_payload}")" != "$(jq -c "${identity_filter}" <<<"${kubelet_after}")" ]]; then
  echo "ERROR: kubelet-plugin Pod identity or restart count changed during the session" >&2
  exit 1
fi
if [[ "${TIER_C_PROVIDER}" == "persistent-agent-v1" && "$(jq -c "${identity_filter}" <<<"${agent_payload}")" != "$(jq -c "${identity_filter}" <<<"${agent_after}")" ]]; then
  echo "ERROR: persistent-agent Pod identity or restart count changed during the session" >&2
  exit 1
fi

{
  cat <<'EOF'
# Persistent-agent Tier C evidence
EOF
  printf '\n- Run: %s\n' "${RUN_ID}"
  printf -- '- Installed source commit: %s\n' "$(git -C "${TIER_C_SOURCE_WORKTREE}" rev-parse HEAD)"
  printf -- '- Harness commit: %s\n' "${HARNESS_SHA}"
  printf -- '- Kubernetes: %s\n' "${server_version}"
  printf -- '- Provider: %s\n' "${TIER_C_PROVIDER}"
  printf -- '- Arm: %s\n' "${TIER_C_ARM}"
  printf -- '- Block: %s\n' "${TIER_C_BLOCK}"
  printf -- '- Scenario: %s\n' "${TIER_C_SCENARIO}"
  printf -- '- Shape: %s\n' "${TIER_C_SHAPE}"
  printf -- '- Trials: %s\n' "${TIER_C_TRIALS}"
  printf -- '- Warm-up trials: %s\n' "${TIER_C_WARMUP_TRIALS}"
  printf -- '- Promotion run: %s\n' "${TIER_C_PROMOTION_RUN}"
  printf -- '- T0 source: %s\n' "${TIER_C_T0_MODE}"
  printf -- '- Profile: %s\n' "${TIER_C_PROFILE}"
  printf -- '- Image cache state: %s\n' "${TIER_C_CACHE_STATE}"
  cat <<'EOF'

The per-trial directories contain raw Pods, claims, Nodes, snapshots,
reservations, Events, timestamped kubelet-plugin logs, metrics snapshots,
`timeline.csv`, and `result.json`. The aggregate directory combines all Pod
samples using the nearest-rank percentile convention.
The JSON result also records deterministic trial-cluster bootstrap p50/p95 95%
confidence intervals, seed, and repetition count.

When `promotion_run=false`, this is directional or harness-validation evidence
only. The current two-Node AWS EKS environment can exercise the real scheduler,
kubelet NodePrepareResources, container readiness, persistent agent, and IMEX,
but it cannot satisfy the required 18/144-node sample sizes. Kind cannot supply
real kubelet/IMEX Tier C evidence at all.
EOF
} > "${ARTIFACTS}/README.md"

trap - EXIT
echo "PASS: persistent-agent Tier C ${TIER_C_PROVIDER} ${TIER_C_SHAPE}"
echo "Artifacts: ${ARTIFACTS}"
