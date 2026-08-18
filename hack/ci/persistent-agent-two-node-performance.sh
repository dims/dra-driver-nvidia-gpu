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

# Runs the balanced, two-arm GB200 performance study. M is an actual pinned
# upstream-main checkout. B is the latest signed persistent-agent branch. The
# caller owns installation details through explicit hooks; no alternate branch
# configuration is accepted as a substitute for M.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SHORT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short=12 HEAD)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-${SHORT_SHA}}"
ARTIFACTS="${ARTIFACTS:-/tmp/persistent-agent-two-node-performance/${RUN_ID}}"

PERF_MAIN_WORKTREE="${PERF_MAIN_WORKTREE:-}"
PERF_BRANCH_WORKTREE="${PERF_BRANCH_WORKTREE:-${REPO_ROOT}}"
PERF_MAIN_SHA="${PERF_MAIN_SHA:-}"
PERF_BRANCH_SHA="${PERF_BRANCH_SHA:-$(git -C "${PERF_BRANCH_WORKTREE}" rev-parse HEAD)}"
PERF_MAIN_REF="${PERF_MAIN_REF:-origin/main}"
PERF_BRANCH_REF="${PERF_BRANCH_REF:-origin/feature/persistent-compute-domain-agents-scale}"
PERF_MAIN_INSTALL_HOOK="${PERF_MAIN_INSTALL_HOOK:-}"
PERF_BRANCH_INSTALL_HOOK="${PERF_BRANCH_INSTALL_HOOK:-}"
PERF_DECOMMISSION_HOOK="${PERF_DECOMMISSION_HOOK:-}"
PERF_TIER_C_RUNNER="${PERF_TIER_C_RUNNER:-${SCRIPT_DIR}/persistent-agent-tier-c-test.sh}"
PERF_NODE_SELECTOR="${PERF_NODE_SELECTOR:-}"
PERF_BLOCKS="${PERF_BLOCKS:-4}"
PERF_TRIALS="${PERF_TRIALS:-25}"
PERF_WARMUPS="${PERF_WARMUPS:-2}"
PERF_PILOT="${PERF_PILOT:-false}"
PERF_ENFORCE="${PERF_ENFORCE:-true}"
PERF_DRIVER_NAMESPACE="${PERF_DRIVER_NAMESPACE:-nvidia-dra-driver-gpu}"
PERF_MAIN_DRIVER_IMAGE="${PERF_MAIN_DRIVER_IMAGE:-}"
PERF_BRANCH_DRIVER_IMAGE="${PERF_BRANCH_DRIVER_IMAGE:-}"
PERF_WORKLOAD_TEMPLATE="${PERF_WORKLOAD_TEMPLATE:-${SCRIPT_DIR}/fixtures/persistent-agent-tier-c/nvbandwidth-workload.yaml.tmpl}"
PERF_WORKLOAD_IMAGE="${PERF_WORKLOAD_IMAGE:-registry.k8s.io/e2e-test-images/agnhost:2.53}"
PERF_SMOKE_SCRIPT="${PERF_SMOKE_SCRIPT:-${SCRIPT_DIR}/persistent-agent-two-node-smoke.sh}"
PERF_DATA_PLANE_SCRIPT="${PERF_DATA_PLANE_SCRIPT:-${SCRIPT_DIR}/persistent-agent-tier-c-nvbandwidth.sh}"
PERF_DATA_PLANE_CADENCE="${PERF_DATA_PLANE_CADENCE:-key}"
PERF_OBSERVABILITY_SCRIPT="${PERF_OBSERVABILITY_SCRIPT:-}"
PERF_T0_MODE="${PERF_T0_MODE:-creationTimestamp}"
PERF_AUDIT_LOG="${PERF_AUDIT_LOG:-}"
PERF_TIMEOUT_SECONDS="${PERF_TIMEOUT_SECONDS:-1800}"
PERF_RETIRE_TIMEOUT_SECONDS="${PERF_RETIRE_TIMEOUT_SECONDS:-600}"
PERF_IDLE_SECONDS="${PERF_IDLE_SECONDS:-900}"
PERF_IDLE_SAMPLE_SECONDS="${PERF_IDLE_SAMPLE_SECONDS:-5}"
KEEP_FAILED_FIXTURE="${KEEP_FAILED_FIXTURE:-true}"

for tool in git go jq kubectl python3 rg; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done
for value in PERF_MAIN_WORKTREE PERF_MAIN_SHA PERF_MAIN_INSTALL_HOOK PERF_BRANCH_INSTALL_HOOK PERF_DECOMMISSION_HOOK PERF_NODE_SELECTOR PERF_MAIN_DRIVER_IMAGE PERF_BRANCH_DRIVER_IMAGE; do
  if [[ -z "${!value}" ]]; then
    echo "ERROR: ${value} is required" >&2
    exit 1
  fi
done
for hook in "${PERF_MAIN_INSTALL_HOOK}" "${PERF_BRANCH_INSTALL_HOOK}" "${PERF_DECOMMISSION_HOOK}"; do
  if [[ ! -x "${hook}" ]]; then
    echo "ERROR: hook ${hook} must be executable" >&2
    exit 1
  fi
done
if [[ ! -f "${PERF_TIER_C_RUNNER}" ]]; then
  echo "ERROR: PERF_TIER_C_RUNNER ${PERF_TIER_C_RUNNER} is not a file" >&2
  exit 1
fi
for optional_hook in "${PERF_SMOKE_SCRIPT}" "${PERF_DATA_PLANE_SCRIPT}" "${PERF_OBSERVABILITY_SCRIPT}"; do
  if [[ -n "${optional_hook}" && ! -x "${optional_hook}" ]]; then
    echo "ERROR: hook ${optional_hook} must be executable" >&2
    exit 1
  fi
done
for value in PERF_BLOCKS PERF_TRIALS PERF_WARMUPS PERF_IDLE_SECONDS PERF_IDLE_SAMPLE_SECONDS; do
  if [[ ! "${!value}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: ${value} must be numeric" >&2
    exit 1
  fi
done
if (( PERF_BLOCKS < 1 || PERF_TRIALS < 1 )); then
  echo "ERROR: PERF_BLOCKS and PERF_TRIALS must be positive" >&2
  exit 1
fi
if (( PERF_IDLE_SAMPLE_SECONDS < 1 )); then
  echo "ERROR: PERF_IDLE_SAMPLE_SECONDS must be positive" >&2
  exit 1
fi
if [[ "${PERF_PILOT}" == "true" ]]; then
  PERF_BLOCKS=1
  PERF_TRIALS=5
  PERF_ENFORCE=false
  if (( PERF_IDLE_SECONDS > 10 )); then
    PERF_IDLE_SECONDS=10
  fi
elif [[ "${PERF_PILOT}" != "false" ]]; then
  echo "ERROR: PERF_PILOT must be true or false" >&2
  exit 1
fi
if [[ "${PERF_ENFORCE}" != "true" && "${PERF_ENFORCE}" != "false" ]]; then
  echo "ERROR: PERF_ENFORCE must be true or false" >&2
  exit 1
fi

actual_main_sha="$(git -C "${PERF_MAIN_WORKTREE}" rev-parse HEAD)"
actual_branch_sha="$(git -C "${PERF_BRANCH_WORKTREE}" rev-parse HEAD)"
if [[ "${actual_main_sha}" != "${PERF_MAIN_SHA}" ]]; then
  echo "ERROR: main worktree HEAD ${actual_main_sha} does not match PERF_MAIN_SHA ${PERF_MAIN_SHA}" >&2
  exit 1
fi
if [[ "$(git -C "${PERF_MAIN_WORKTREE}" rev-parse "${PERF_MAIN_REF}")" != "${PERF_MAIN_SHA}" ]]; then
  echo "ERROR: PERF_MAIN_REF ${PERF_MAIN_REF} does not resolve to the pinned main SHA" >&2
  exit 1
fi
if [[ "${actual_branch_sha}" != "${PERF_BRANCH_SHA}" ]]; then
  echo "ERROR: branch worktree HEAD ${actual_branch_sha} does not match PERF_BRANCH_SHA ${PERF_BRANCH_SHA}" >&2
  exit 1
fi
if [[ "$(git -C "${PERF_BRANCH_WORKTREE}" rev-parse "${PERF_BRANCH_REF}")" != "${PERF_BRANCH_SHA}" ]]; then
  echo "ERROR: PERF_BRANCH_REF ${PERF_BRANCH_REF} does not resolve to the latest branch SHA" >&2
  exit 1
fi
if [[ "${actual_main_sha}" == "${actual_branch_sha}" ]]; then
  echo "ERROR: M and B resolve to the same commit" >&2
  exit 1
fi
if [[ -n "$(git -C "${PERF_MAIN_WORKTREE}" status --porcelain=v1)" || -n "$(git -C "${PERF_BRANCH_WORKTREE}" status --porcelain=v1)" ]]; then
  echo "ERROR: both source worktrees must be clean" >&2
  exit 1
fi
git -C "${PERF_BRANCH_WORKTREE}" verify-commit "${PERF_BRANCH_SHA}" > /dev/null

mkdir -p "${ARTIFACTS}/source" "${ARTIFACTS}/blocks" "${ARTIFACTS}/comparison"
manifest="${ARTIFACTS}/manifest.csv"
installations="${ARTIFACTS}/installations.csv"
printf '%s\n' 'block,arm,scenario,result,lifecycle,artifacts' > "${manifest}"
printf '%s\n' 'block,arm,duration_ms' > "${installations}"
git -C "${PERF_MAIN_WORKTREE}" log -1 --show-signature --format=fuller "${PERF_MAIN_SHA}" > "${ARTIFACTS}/source/main.txt" 2>&1
git -C "${PERF_BRANCH_WORKTREE}" log -1 --show-signature --format=fuller "${PERF_BRANCH_SHA}" > "${ARTIFACTS}/source/branch.txt" 2>&1
git -C "${PERF_MAIN_WORKTREE}" remote -v > "${ARTIFACTS}/source/main-remotes.txt"
git -C "${PERF_BRANCH_WORKTREE}" remote -v > "${ARTIFACTS}/source/branch-remotes.txt"
kubectl version -o yaml > "${ARTIFACTS}/source/kubernetes.yaml"
kubectl get nodes -l "${PERF_NODE_SELECTOR}" -o yaml > "${ARTIFACTS}/source/nodes.yaml"
{
  echo "run_id=${RUN_ID}"
  echo "main_sha=${PERF_MAIN_SHA}"
  echo "main_ref=${PERF_MAIN_REF}"
  echo "branch_sha=${PERF_BRANCH_SHA}"
  echo "branch_ref=${PERF_BRANCH_REF}"
  echo "main_driver_image=${PERF_MAIN_DRIVER_IMAGE}"
  echo "branch_driver_image=${PERF_BRANCH_DRIVER_IMAGE}"
  echo "blocks=${PERF_BLOCKS}"
  echo "trials_per_block=${PERF_TRIALS}"
  echo "warmups_per_install=${PERF_WARMUPS}"
  echo "node_selector=${PERF_NODE_SELECTOR}"
  echo "t0_mode=${PERF_T0_MODE}"
} > "${ARTIFACTS}/source/run.txt"

failed=true
on_exit() {
  local rc=$?
  if [[ "${failed}" == "true" && ${rc} -ne 0 ]]; then
    echo "FAILED: preserving the current installation and ${ARTIFACTS}" >&2
  fi
}
trap on_exit EXIT

resource_count_or_absent() {
  local resource="$1"
  shift
  local output error_file
  error_file="${ARTIFACTS}/source/pristine-${resource//\//-}.error"
  if output="$(kubectl get "${resource}" "$@" -o json 2> "${error_file}")"; then
    jq -r '.items | length' <<<"${output}"
    return 0
  fi
  if rg -qi 'the server (does not have|could not find) the requested resource|no matches for kind' "${error_file}"; then
    echo 0
    return 0
  fi
  cat "${error_file}" >&2
  return 1
}

assert_protocol_pristine() {
  local phase="$1"
  local domains agents node_payload residue
  domains="$(resource_count_or_absent computedomains -A)"
  agents="$(resource_count_or_absent pods -A -l resource.nvidia.com/persistentComputeDomainAgent=true)"
  node_payload="$(kubectl get nodes -l "${PERF_NODE_SELECTOR}" -o json)"
  residue="$(jq '[.items[] | select(
    (.metadata.labels["resource.nvidia.com/computeDomain"] // "") != "" or
    (.metadata.labels["resource.nvidia.com/persistentAgentComputeDomain"] // "") != "" or
    (.metadata.annotations["resource.nvidia.com/computeDomainAttestation"] // "") != "" or
    (.metadata.annotations["resource.nvidia.com/computeDomainCliqueRetirementFenced"] // "") != ""
  )] | length' <<<"${node_payload}")"
  if [[ "${domains}" != "0" || "${agents}" != "0" || "${residue}" != "0" ]]; then
    echo "ERROR: ${phase} is not pristine: ComputeDomains=${domains}, agents=${agents}, selected-Node protocol residue=${residue}" >&2
    return 1
  fi
}

capture_idle_usage() {
  local directory="$1"
  mkdir -p "${directory}"
  if ! kubectl top pods -n "${PERF_DRIVER_NAMESPACE}" --containers --no-headers > "${directory}/probe.txt" 2> "${directory}/unavailable.txt"; then
    printf '%s\n' 'unavailable: metrics.k8s.io or authorization did not permit kubectl top' > "${directory}/status.txt"
    return 0
  fi
  printf '%s\n' 'timestamp_utc output' > "${directory}/samples.txt"
  local elapsed=0
  while (( elapsed <= PERF_IDLE_SECONDS )); do
    printf '%s ' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${directory}/samples.txt"
    if ! kubectl top pods -n "${PERF_DRIVER_NAMESPACE}" --containers --no-headers >> "${directory}/samples.txt" 2>> "${directory}/unavailable.txt"; then
      printf '%s\n' 'partial: metrics collection failed after sampling began' > "${directory}/status.txt"
      return 0
    fi
    if (( elapsed == PERF_IDLE_SECONDS )); then
      break
    fi
    sleep "${PERF_IDLE_SAMPLE_SECONDS}"
    elapsed=$((elapsed + PERF_IDLE_SAMPLE_SECONDS))
    if (( elapsed > PERF_IDLE_SECONDS )); then
      elapsed="${PERF_IDLE_SECONDS}"
    fi
  done
  printf '%s\n' 'complete' > "${directory}/status.txt"
}

invoke_hook() {
  local hook="$1"
  local action="$2"
  local arm="$3"
  local block="$4"
  local source_worktree="$5"
  local source_sha="$6"
  local driver_image="$7"
  local directory="$8"
  local started_ms completed_ms
  mkdir -p "${directory}"
  started_ms="$(python3 -c 'import time; print(time.time_ns() // 1_000_000)')"
  PERF_ACTION="${action}" PERF_ARM="${arm}" PERF_BLOCK="${block}" \
  PERF_SOURCE_WORKTREE="${source_worktree}" PERF_SOURCE_SHA="${source_sha}" \
  PERF_DRIVER_IMAGE="${driver_image}" PERF_DRIVER_NAMESPACE="${PERF_DRIVER_NAMESPACE}" \
  PERF_SESSION_ARTIFACTS="${directory}" \
    "${hook}" > "${directory}/${action}.log" 2>&1
  completed_ms="$(python3 -c 'import time; print(time.time_ns() // 1_000_000)')"
  jq -n --arg arm "${arm}" --arg action "${action}" --argjson block "${block}" \
    --argjson startedAtMS "${started_ms}" --argjson completedAtMS "${completed_ms}" \
    '{arm:$arm,action:$action,block:$block,startedAtMS:$startedAtMS,completedAtMS:$completedAtMS,durationMS:($completedAtMS-$startedAtMS)}' \
    > "${directory}/timing.json"
}

run_scenario() {
  local block="$1"
  local arm="$2"
  local provider="$3"
  local scenario="$4"
  local source_worktree="$5"
  local directory="$6"

  RUN_ID="${RUN_ID}-b${block}-${arm}-${scenario}" \
  ARTIFACTS="${directory}" \
  TIER_C_SOURCE_WORKTREE="${source_worktree}" \
  TIER_C_ARM="${arm}" \
  TIER_C_BLOCK="${block}" \
  TIER_C_PROVIDER="${provider}" \
  TIER_C_SCENARIO="${scenario}" \
  TIER_C_SHAPE=1x2 \
  TIER_C_TRIALS="${PERF_TRIALS}" \
  TIER_C_WARMUP_TRIALS="${PERF_WARMUPS}" \
  TIER_C_PROMOTION_RUN=false \
  TIER_C_NODE_SELECTOR="${PERF_NODE_SELECTOR}" \
  TIER_C_DRIVER_NAMESPACE="${PERF_DRIVER_NAMESPACE}" \
  TIER_C_T0_MODE="${PERF_T0_MODE}" \
  TIER_C_AUDIT_LOG="${PERF_AUDIT_LOG}" \
  TIER_C_PROFILE=directional \
  TIER_C_CACHE_STATE=unspecified \
  TIER_C_WORKLOAD_TEMPLATE="${PERF_WORKLOAD_TEMPLATE}" \
  TIER_C_WORKLOAD_IMAGE="${PERF_WORKLOAD_IMAGE}" \
  TIER_C_SMOKE_SCRIPT="${PERF_SMOKE_SCRIPT}" \
  TIER_C_DATA_PLANE_SCRIPT="${PERF_DATA_PLANE_SCRIPT}" \
  TIER_C_DATA_PLANE_CADENCE="${PERF_DATA_PLANE_CADENCE}" \
  TIER_C_OBSERVABILITY_SCRIPT="${PERF_OBSERVABILITY_SCRIPT}" \
  TIER_C_TIMEOUT_SECONDS="${PERF_TIMEOUT_SECONDS}" \
  TIER_C_RETIRE_TIMEOUT_SECONDS="${PERF_RETIRE_TIMEOUT_SECONDS}" \
  KEEP_FAILED_FIXTURE="${KEEP_FAILED_FIXTURE}" \
    bash "${PERF_TIER_C_RUNNER}"

  local result="${directory}/${provider}/1x2/aggregate/result.json"
  local lifecycle="${directory}/lifecycle.jsonl"
  local measured="${directory}/${provider}/1x2/measured"
  if [[ ! -s "${result}" || ! -s "${lifecycle}" || ! -d "${measured}" ]]; then
    echo "ERROR: ${arm}/${scenario} did not produce its result, lifecycle, and measured artifacts" >&2
    exit 1
  fi
  printf '%s,%s,%s,%s,%s,%s\n' "${block}" "${arm}" "${scenario}" "${result}" "${lifecycle}" "${measured}" >> "${manifest}"
}

run_arm() {
  local block="$1"
  local arm="$2"
  local install_hook provider source_worktree source_sha driver_image
  if [[ "${arm}" == "M" ]]; then
    install_hook="${PERF_MAIN_INSTALL_HOOK}"
    provider="main"
    source_worktree="${PERF_MAIN_WORKTREE}"
    source_sha="${PERF_MAIN_SHA}"
    driver_image="${PERF_MAIN_DRIVER_IMAGE}"
  else
    install_hook="${PERF_BRANCH_INSTALL_HOOK}"
    provider="persistent-agent-v1"
    source_worktree="${PERF_BRANCH_WORKTREE}"
    source_sha="${PERF_BRANCH_SHA}"
    driver_image="${PERF_BRANCH_DRIVER_IMAGE}"
  fi
  local arm_dir
  arm_dir="${ARTIFACTS}/blocks/$(printf '%02d' "${block}")/${arm}"
  assert_protocol_pristine "before block ${block} arm ${arm}"
  invoke_hook "${install_hook}" install "${arm}" "${block}" "${source_worktree}" "${source_sha}" "${driver_image}" "${arm_dir}/installation"
  printf '%s,%s,%s\n' "${block}" "${arm}" "$(jq -r '.durationMS' "${arm_dir}/installation/timing.json")" >> "${installations}"
  kubectl get pods -n "${PERF_DRIVER_NAMESPACE}" -o json > "${arm_dir}/installation/pods.json"
  if (( block == 1 )); then
    capture_idle_usage "${arm_dir}/idle"
  fi

  if (( block % 2 == 1 )); then
    run_scenario "${block}" "${arm}" "${provider}" cold-domain "${source_worktree}" "${arm_dir}/cold-domain"
    run_scenario "${block}" "${arm}" "${provider}" warm-workload "${source_worktree}" "${arm_dir}/warm-workload"
  else
    run_scenario "${block}" "${arm}" "${provider}" warm-workload "${source_worktree}" "${arm_dir}/warm-workload"
    run_scenario "${block}" "${arm}" "${provider}" cold-domain "${source_worktree}" "${arm_dir}/cold-domain"
  fi

  invoke_hook "${PERF_DECOMMISSION_HOOK}" decommission "${arm}" "${block}" "${source_worktree}" "${source_sha}" "${driver_image}" "${arm_dir}/decommission"
  assert_protocol_pristine "after block ${block} arm ${arm}"
}

for ((block = 1; block <= PERF_BLOCKS; block++)); do
  if (( block % 2 == 1 )); then
    run_arm "${block}" M
    run_arm "${block}" B
  else
    run_arm "${block}" B
    run_arm "${block}" M
  fi
done

comparison_args=(
  --manifest "${manifest}"
  --installations "${installations}"
  --output-dir "${ARTIFACTS}/comparison"
  --expected-blocks "${PERF_BLOCKS}"
  --expected-trials "${PERF_TRIALS}"
)
if [[ "${PERF_ENFORCE}" == "true" ]]; then
  comparison_args+=(--enforce)
fi
go run ./hack/tools/persistent-agent-comparison "${comparison_args[@]}"

failed=false
trap - EXIT
echo "PASS: main versus latest-branch two-Node performance run"
echo "Report: ${ARTIFACTS}/comparison/report.md"
