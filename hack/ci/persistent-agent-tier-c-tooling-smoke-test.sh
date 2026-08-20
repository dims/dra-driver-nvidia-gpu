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

# Covers the Tier C name, log-retry, and fast-workload smoke race regressions
# without requiring Kubernetes or a GPU.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES="${SCRIPT_DIR}/fixtures/persistent-agent-two-node-performance"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# shellcheck source=hack/ci/persistent-agent-tier-c-lib.sh
source "${SCRIPT_DIR}/persistent-agent-tier-c-lib.sh"

long_name='pa-b-b123-persistent-agent-warmworkload-81b7ce656fde-23136-'
session_id="$(truncate_dns_label "${long_name}" 52)"
workload_name="$(truncate_dns_label "workload-${long_name}" 52)"
worker_suffix='-worker-143'
[[ "${session_id}" != *- && "${workload_name}" != *- ]]
(( ${#session_id} <= 52 && ${#workload_name} <= 52 ))
(( ${#workload_name} + ${#worker_suffix} <= 63 ))

mkdir -p "${TMP_DIR}/bin" "${TMP_DIR}/state"
ln -s "${FIXTURES}/kubectl-tooling" "${TMP_DIR}/bin/kubectl"
export PATH="${TMP_DIR}/bin:${PATH}"
export MOCK_STATE_DIR="${TMP_DIR}/state"

export MOCK_LOG_SUCCEED_ON=3
log_payload="$(kubectl_logs_with_retry 3 0 -n driver kubelet -c compute-domains)"
[[ "${log_payload}" == "complete log payload" ]]
[[ "$(<"${MOCK_STATE_DIR}/log-attempts")" == "3" ]]

rm -f "${MOCK_STATE_DIR}/log-attempts"
export MOCK_LOG_PERMANENT=true
previous_payload="$(kubectl_logs_with_retry 3 0 -n driver kubelet -c compute-domains --previous)"
[[ -z "${previous_payload}" ]]
[[ "$(<"${MOCK_STATE_DIR}/log-attempts")" == "1" ]]
unset MOCK_LOG_PERMANENT MOCK_LOG_SUCCEED_ON

export MOCK_COMPUTEDOMAIN_READY_ON=3
wait_for_computedomain_ready tooling-smoke domain 5 0
[[ "$(<"${MOCK_STATE_DIR}/computedomain-gets")" == "3" ]]
unset MOCK_COMPUTEDOMAIN_READY_ON

for smoke_case in success completed notfound; do
  MOCK_SMOKE_CASE="${smoke_case}" \
  TIER_C_TRIAL_NAMESPACE=tooling-smoke \
  TIER_C_POD_SELECTOR=tooling-smoke=true \
  EXPECTED_PODS=2 \
    bash "${SCRIPT_DIR}/persistent-agent-two-node-smoke.sh" \
      > "${TMP_DIR}/smoke-${smoke_case}.log" 2>&1
done

if MOCK_SMOKE_CASE=running \
   TIER_C_TRIAL_NAMESPACE=tooling-smoke \
   TIER_C_POD_SELECTOR=tooling-smoke=true \
   EXPECTED_PODS=2 \
     bash "${SCRIPT_DIR}/persistent-agent-two-node-smoke.sh" \
       > "${TMP_DIR}/smoke-running.log" 2>&1; then
  echo "ERROR: smoke accepted an exec failure from a still-running container" >&2
  exit 1
fi
rg -q 'still Running' "${TMP_DIR}/smoke-running.log"

echo "PASS: Tier C tooling regressions"
