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

truncate_dns_label() {
  local value="$1"
  local max_length="$2"

  value="${value:0:${max_length}}"
  while [[ "${value}" == *- ]]; do
    value="${value%-}"
  done
  if [[ -z "${value}" ]]; then
    echo "ERROR: truncation produced an empty DNS label" >&2
    return 1
  fi
  printf '%s\n' "${value}"
}

wait_for_computedomain_ready() {
  local namespace="$1"
  local name="$2"
  local timeout_seconds="$3"
  local poll_seconds="${4:-2}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    local payload status
    if payload="$(kubectl get computedomain -n "${namespace}" "${name}" -o json 2>/dev/null)"; then
      status="$(jq -r '.status.status // ""' <<<"${payload}")"
      if [[ "${status}" == "Ready" ]]; then
        return 0
      fi
    fi
    sleep "${poll_seconds}"
  done
  echo "ERROR: ComputeDomain ${namespace}/${name} did not reach Ready within ${timeout_seconds}s" >&2
  return 1
}

kubectl_logs_with_retry() {
  local attempts="$1"
  local delay_seconds="$2"
  shift 2

  local attempt error_file output_file
  output_file="$(mktemp)"
  error_file="$(mktemp)"
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if kubectl logs "$@" > "${output_file}" 2> "${error_file}"; then
      cat "${output_file}"
      rm -f "${output_file}" "${error_file}"
      return 0
    fi

    # A container without a prior instance is an expected, permanent result.
    # Retrying it on every trial adds minutes of avoidable delay at scale.
    if [[ " $* " == *" --previous "* ]] &&
       rg -qi 'no previous terminated container|previous terminated container .* not found' "${error_file}"; then
      rm -f "${output_file}" "${error_file}"
      return 0
    fi
    if (( attempt < attempts )); then
      sleep "${delay_seconds}"
    fi
  done

  echo "ERROR: kubectl logs failed after ${attempt} attempt(s): kubectl logs $*" >&2
  cat "${error_file}" >&2
  rm -f "${output_file}" "${error_file}"
  return 1
}
