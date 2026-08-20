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

# Performs the cheap, per-cycle data-path smoke for the two-node comparison.
# The heavier nvbandwidth validation runs only at the configured cadence.
set -o errexit
set -o nounset
set -o pipefail

: "${TIER_C_TRIAL_NAMESPACE:?TIER_C_TRIAL_NAMESPACE is required}"
: "${TIER_C_POD_SELECTOR:?TIER_C_POD_SELECTOR is required}"
: "${EXPECTED_PODS:?EXPECTED_PODS is required}"

SMOKE_CONTAINER="${TIER_C_SMOKE_CONTAINER:-mpi-worker}"
pods=()
verified=0
completed=0
while IFS= read -r pod; do
  pods+=("${pod}")
done < <(kubectl get pods -n "${TIER_C_TRIAL_NAMESPACE}" -l "${TIER_C_POD_SELECTOR}" -o json | jq -r '.items[].metadata.name' | sort)

if (( ${#pods[@]} != EXPECTED_PODS )); then
  echo "ERROR: smoke selected ${#pods[@]} Pods, want ${EXPECTED_PODS}" >&2
  exit 1
fi

for pod in "${pods[@]}"; do
  exec_error="$(mktemp)"
  if kubectl exec -n "${TIER_C_TRIAL_NAMESPACE}" "${pod}" -c "${SMOKE_CONTAINER}" -- \
    sh -ceu 'test -e /dev/nvidiactl; nvidia-smi -L | grep -q "^GPU "' 2> "${exec_error}"; then
    verified=$((verified + 1))
    rm -f "${exec_error}"
    continue
  fi

  pod_error="$(mktemp)"
  if ! pod_payload="$(kubectl get pod -n "${TIER_C_TRIAL_NAMESPACE}" "${pod}" -o json 2> "${pod_error}")"; then
    if rg -qi 'notfound|not found' "${pod_error}"; then
      echo "INFO: ${pod} completed and was removed before the device smoke finished"
      completed=$((completed + 1))
      rm -f "${exec_error}" "${pod_error}"
      continue
    fi
    cat "${exec_error}" >&2
    cat "${pod_error}" >&2
    rm -f "${exec_error}" "${pod_error}"
    exit 1
  fi
  rm -f "${pod_error}"

  phase="$(jq -r '.status.phase // ""' <<<"${pod_payload}")"
  container_running="$(jq -r --arg container "${SMOKE_CONTAINER}" \
    'any(.status.containerStatuses[]?; .name == $container and .state.running != null)' <<<"${pod_payload}")"
  if [[ "${phase}" != "Running" || "${container_running}" != "true" ]]; then
    echo "INFO: ${pod} completed before the device smoke finished (phase=${phase:-unknown})"
    completed=$((completed + 1))
    rm -f "${exec_error}"
    continue
  fi

  echo "ERROR: device smoke failed while ${pod}/${SMOKE_CONTAINER} was still Running" >&2
  cat "${exec_error}" >&2
  rm -f "${exec_error}"
  exit 1
done

echo "PASS: ${verified} workload Pods exposed an operational NVIDIA device; ${completed} completed before inspection"
