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

# Tier C data-plane hook for the accompanying MPIJob template. The parent
# runner supplies every TIER_C_* variable below.
set -o errexit
set -o nounset
set -o pipefail

: "${TIER_C_TRIAL_NAMESPACE:?parent runner must set TIER_C_TRIAL_NAMESPACE}"
: "${TIER_C_WORKLOAD_NAME:?parent runner must set TIER_C_WORKLOAD_NAME}"
: "${TIER_C_TRIAL_ARTIFACTS:?parent runner must set TIER_C_TRIAL_ARTIFACTS}"

NVB_TIMEOUT_SECONDS="${NVB_TIMEOUT_SECONDS:-1800}"
launcher_job="${TIER_C_WORKLOAD_NAME}-launcher"

kubectl wait -n "${TIER_C_TRIAL_NAMESPACE}" \
  --for=create "job/${launcher_job}" \
  --timeout="${NVB_TIMEOUT_SECONDS}s"
kubectl wait -n "${TIER_C_TRIAL_NAMESPACE}" \
  --for=condition=Complete "job/${launcher_job}" \
  --timeout="${NVB_TIMEOUT_SECONDS}s"
kubectl logs -n "${TIER_C_TRIAL_NAMESPACE}" \
  "job/${launcher_job}" --timestamps \
  > "${TIER_C_TRIAL_ARTIFACTS}/nvbandwidth.log"

if ! rg -q 'multinode_device_to_device_memcpy_read_ce' "${TIER_C_TRIAL_ARTIFACTS}/nvbandwidth.log"; then
  echo "ERROR: nvbandwidth output does not identify the required cross-node test" >&2
  exit 1
fi
if rg -qi '(^|[^a-z])(error|failed|failure)([^a-z]|$)' "${TIER_C_TRIAL_ARTIFACTS}/nvbandwidth.log"; then
  echo "ERROR: nvbandwidth output contains an error/failure marker" >&2
  exit 1
fi

printf '%s\n' 'pass' > "${TIER_C_TRIAL_ARTIFACTS}/data-plane-result.txt"
echo "PASS: nvbandwidth cross-node data-plane hook"
