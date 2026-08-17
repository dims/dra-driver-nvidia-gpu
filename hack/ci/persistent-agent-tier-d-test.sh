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

# Runs the Tier D 280x18 virtual-node control-plane profile over an explicit
# stable-Kubernetes Kind image matrix. It reuses the independently verified
# Tier B real-API gate and adds version orchestration and resource sampling.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SHORT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short=12 HEAD)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-${SHORT_SHA}}"
ARTIFACTS="${ARTIFACTS:-/tmp/persistent-agent-tier-d/${RUN_ID}}"
TIER_D_KIND_NODE_IMAGES="${TIER_D_KIND_NODE_IMAGES:-kindest/node:v1.34.0}"
TIER_D_SHAPE="${TIER_D_SHAPE:-280x18}"
TIER_D_PROMOTION_RUN="${TIER_D_PROMOTION_RUN:-false}"
TIER_D_FIXTURE_WORKERS="${TIER_D_FIXTURE_WORKERS:-32}"
TIER_D_TIMEOUT_SECONDS="${TIER_D_TIMEOUT_SECONDS:-1800}"
TIER_D_KEEP_FAILED_CLUSTER="${TIER_D_KEEP_FAILED_CLUSTER:-true}"
TIER_D_MATCHED_KUBECTL="${TIER_D_MATCHED_KUBECTL:-true}"

for tool in cut date docker git go jq kind kubectl tee tr uname; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done
if [[ "${TIER_D_MATCHED_KUBECTL}" == "true" ]] && ! command -v curl > /dev/null 2>&1; then
  echo "ERROR: curl is required to fetch the version-matched kubectl" >&2
  exit 1
fi
if [[ "${TIER_D_MATCHED_KUBECTL}" == "true" ]] && ! command -v sha256sum > /dev/null 2>&1 && ! command -v shasum > /dev/null 2>&1; then
  echo "ERROR: sha256sum or shasum is required to verify kubectl" >&2
  exit 1
fi

if [[ "${TIER_D_PROMOTION_RUN}" == "true" && "${TIER_D_SHAPE}" != "280x18" ]]; then
  echo "ERROR: a Tier D promotion run must use TIER_D_SHAPE=280x18" >&2
  exit 1
fi
if [[ "${TIER_D_PROMOTION_RUN}" == "true" && "${TIER_D_MATCHED_KUBECTL}" != "true" ]]; then
  echo "ERROR: a Tier D promotion run requires a version-matched kubectl" >&2
  exit 1
fi

read -r -a node_images <<<"${TIER_D_KIND_NODE_IMAGES}"
if (( ${#node_images[@]} == 0 )); then
  echo "ERROR: TIER_D_KIND_NODE_IMAGES is empty" >&2
  exit 1
fi
if [[ "${TIER_D_PROMOTION_RUN}" == "true" && ${#node_images[@]} -lt 2 ]]; then
  echo "ERROR: a Tier D promotion run requires explicit oldest and newest supported stable Kind images" >&2
  exit 1
fi
for image in "${node_images[@]}"; do
  if [[ ! "${image}" =~ ^kindest/node:v1\.[0-9]+\.[0-9]+(@sha256:[0-9a-f]{64})?$ ]]; then
    echo "ERROR: '${image}' is not an explicit stable kindest/node patch image (optionally pinned by sha256 digest)" >&2
    exit 1
  fi
  if [[ "${TIER_D_PROMOTION_RUN}" == "true" && "${image}" != *@sha256:* ]]; then
    echo "ERROR: a Tier D promotion run must pin every Kind image by sha256 digest" >&2
    exit 1
  fi
done

mkdir -p "${ARTIFACTS}"
{
  echo "run_id=${RUN_ID}"
  echo "commit=$(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "branch=$(git -C "${REPO_ROOT}" branch --show-current)"
  echo "dirty_files=$(git -C "${REPO_ROOT}" status --porcelain=v1 | wc -l | tr -d ' ')"
  echo "shape=${TIER_D_SHAPE}"
  echo "promotion_run=${TIER_D_PROMOTION_RUN}"
  echo "node_images=${TIER_D_KIND_NODE_IMAGES}"
  echo "matched_kubectl=${TIER_D_MATCHED_KUBECTL}"
} > "${ARTIFACTS}/source.txt"
git -C "${REPO_ROOT}" log -1 --show-signature --format=fuller > "${ARTIFACTS}/signature.txt" 2>&1

printf '%s\n' 'image,kubernetes,nodes,cliques,active_seconds,ready_seconds,actions,writes,conflicts,throttled,request_body_bytes,response_body_bytes,watch_body_bytes' > "${ARTIFACTS}/matrix.csv"

active_cluster=""
child_pid=""
sampler_pid=""

cleanup() {
  local rc=$?
  if [[ -n "${sampler_pid}" ]]; then
    kill "${sampler_pid}" > /dev/null 2>&1 || true
    wait "${sampler_pid}" > /dev/null 2>&1 || true
  fi
  if [[ -n "${child_pid}" ]]; then
    kill "${child_pid}" > /dev/null 2>&1 || true
    wait "${child_pid}" > /dev/null 2>&1 || true
  fi
  if [[ -n "${active_cluster}" && ( ${rc} -eq 0 || "${TIER_D_KEEP_FAILED_CLUSTER}" != "true" ) ]]; then
    kind delete cluster --name "${active_cluster}" > /dev/null 2>&1 || true
  elif [[ -n "${active_cluster}" ]]; then
    echo "FAILED: preserving Kind cluster ${active_cluster}" >&2
  fi
  exit "${rc}"
}
trap cleanup EXIT

sample_resources() {
  local cluster="$1"
  local destination="$2"
  printf '%s\n' 'timestamp,container,cpu_percent,memory_usage,net_io,block_io,pids' > "${destination}"
  while true; do
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker stats --no-stream --format '{{json .}}' 2>/dev/null | while IFS= read -r record; do
      name="$(jq -r '.Name' <<<"${record}")"
      if [[ "${name}" == "${cluster}-"* ]]; then
        printf '%s,%s,%s,%s,%s,%s,%s\n' \
          "${timestamp}" \
          "${name}" \
          "$(jq -r '.CPUPerc' <<<"${record}")" \
          "$(jq -r '.MemUsage' <<<"${record}")" \
          "$(jq -r '.NetIO' <<<"${record}")" \
          "$(jq -r '.BlockIO' <<<"${record}")" \
          "$(jq -r '.PIDs' <<<"${record}")" >> "${destination}"
      fi
    done
    sleep 2
  done
}

install_matched_kubectl() {
  local version="$1"
  local destination="$2"
  local os architecture expected actual
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$(uname -m)" in
    arm64|aarch64) architecture=arm64 ;;
    x86_64|amd64) architecture=amd64 ;;
    *) echo "ERROR: unsupported kubectl architecture $(uname -m)" >&2; return 1 ;;
  esac
  mkdir -p "${destination}"
  curl -fsSL "https://dl.k8s.io/release/${version}/bin/${os}/${architecture}/kubectl" -o "${destination}/kubectl"
  expected="$(curl -fsSL "https://dl.k8s.io/release/${version}/bin/${os}/${architecture}/kubectl.sha256")"
  if command -v sha256sum > /dev/null 2>&1; then
    actual="$(sha256sum "${destination}/kubectl" | cut -d ' ' -f 1)"
  else
    actual="$(shasum -a 256 "${destination}/kubectl" | cut -d ' ' -f 1)"
  fi
  if [[ "${actual}" != "${expected}" ]]; then
    echo "ERROR: kubectl ${version} sha256 mismatch" >&2
    return 1
  fi
  chmod +x "${destination}/kubectl"
}

index=0
for image in "${node_images[@]}"; do
  index=$((index + 1))
  version_ref="${image#kindest/node:}"
  version="${version_ref%%@*}"
  version_dir="${ARTIFACTS}/${version}"
  active_cluster="pa-tier-d-${index}-${SHORT_SHA}"
  mkdir -p "${version_dir}"

  version_path="${PATH}"
  if [[ "${TIER_D_MATCHED_KUBECTL}" == "true" ]]; then
    install_matched_kubectl "${version}" "${version_dir}/bin"
    version_path="${version_dir}/bin:${PATH}"
    PATH="${version_path}" kubectl version --client -o yaml > "${version_dir}/kubectl-client.yaml"
  fi

  kind delete cluster --name "${active_cluster}" > /dev/null 2>&1 || true
  sample_resources "${active_cluster}" "${version_dir}/resource-usage.csv" &
  sampler_pid=$!

  PATH="${version_path}" \
  ARTIFACTS="${version_dir}" \
  KIND_CLUSTER_NAME="${active_cluster}" \
  KIND_NODE_IMAGE="${image}" \
  SCALE_SHAPES="${TIER_D_SHAPE}" \
  SCALE_FIXTURE_WORKERS="${TIER_D_FIXTURE_WORKERS}" \
  SCALE_TIMEOUT_SECONDS="${TIER_D_TIMEOUT_SECONDS}" \
  KEEP_CLUSTER=true \
    bash "${SCRIPT_DIR}/persistent-agent-real-api-scale-test.sh" \
      > >(tee "${version_dir}/run.log") 2>&1 &
  child_pid=$!
  if ! wait "${child_pid}"; then
    child_pid=""
    echo "ERROR: Tier D run failed for ${image}" >&2
    exit 1
  fi
  child_pid=""
  kill "${sampler_pid}" > /dev/null 2>&1 || true
  wait "${sampler_pid}" > /dev/null 2>&1 || true
  sampler_pid=""

  result_path="${version_dir}/${TIER_D_SHAPE}/result.json"
  if [[ ! -s "${result_path}" ]]; then
    echo "ERROR: missing ${result_path}" >&2
    exit 1
  fi
  if [[ "$(jq -r '.conflicts' "${result_path}")" != "0" || "$(jq -r '.throttled' "${result_path}")" != "0" ]]; then
    echo "ERROR: healthy Tier D run had conflicts or throttling for ${image}" >&2
    exit 1
  fi
  kubernetes="$(PATH="${version_path}" KUBECONFIG="${version_dir}/kubeconfig" kubectl version -o json | jq -r '.serverVersion.gitVersion')"
  jq -r --arg image "${image}" --arg kubernetes "${kubernetes}" '
    [$image,$kubernetes,.nodes,.cliques,.startToActiveSeconds,.startToReadySeconds,
     .totalControllerActions,.totalControllerWrites,.conflicts,.throttled,
     .transport.requestBodyBytes,.transport.responseBodyBytes,.transport.watchBodyBytes] | @csv
  ' "${result_path}" >> "${ARTIFACTS}/matrix.csv"
  kind export logs "${version_dir}/kind-logs" --name "${active_cluster}" > /dev/null 2>&1 || true
  kind delete cluster --name "${active_cluster}" > /dev/null 2>&1
  active_cluster=""
done

{
  cat <<'EOF'
# Persistent-agent Tier D control-plane evidence
EOF
  printf '\n- Run: %s\n' "${RUN_ID}"
  printf -- '- Commit: %s\n' "$(git -C "${REPO_ROOT}" rev-parse HEAD)"
  printf -- '- Shape: %s\n' "${TIER_D_SHAPE}"
  printf -- '- Kind images: %s\n' "${TIER_D_KIND_NODE_IMAGES}"
  printf -- '- Promotion run: %s\n' "${TIER_D_PROMOTION_RUN}"
  cat <<'EOF'

Each version directory contains the complete Tier B real-API bundle plus
`resource-usage.csv`. The top-level `matrix.csv` compares exact actions,
writes, conflicts, throttling, convergence, and HTTP/watch bytes.

This is virtual-node control-plane evidence. It does not run a real scheduler,
kubelet NodePrepareResources, workload containers, NVML, or IMEX. Tier C and
the largest available genuine-node run supply that complementary evidence.
The default single-v1.34 invocation is a locally available harness check, not
the required oldest/newest-supported-minor promotion result.
EOF
} > "${ARTIFACTS}/README.md"

trap - EXIT
echo "PASS: persistent-agent Tier D matrix"
echo "Artifacts: ${ARTIFACTS}"
