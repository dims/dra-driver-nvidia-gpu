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

# Exercises two-arm ordering, hook propagation, manifests, and the real report
# generator without claiming any Kubernetes or performance evidence.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
FIXTURES="${SCRIPT_DIR}/fixtures/persistent-agent-two-node-performance"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

git clone --quiet "${REPO_ROOT}" "${TMP_DIR}/branch"
git clone --quiet "${REPO_ROOT}" "${TMP_DIR}/main"
git -C "${TMP_DIR}/main" checkout --quiet HEAD^
main_sha="$(git -C "${TMP_DIR}/main" rev-parse HEAD)"
branch_sha="$(git -C "${TMP_DIR}/branch" rev-parse HEAD)"

mkdir -p "${TMP_DIR}/bin"
ln -s "${FIXTURES}/kubectl" "${TMP_DIR}/bin/kubectl"

cd "${REPO_ROOT}"
PATH="${TMP_DIR}/bin:${PATH}" \
ARTIFACTS="${TMP_DIR}/artifacts" \
PERF_PILOT=true \
PERF_MAIN_WORKTREE="${TMP_DIR}/main" \
PERF_MAIN_SHA="${main_sha}" \
PERF_MAIN_REF=HEAD \
PERF_BRANCH_WORKTREE="${TMP_DIR}/branch" \
PERF_BRANCH_SHA="${branch_sha}" \
PERF_BRANCH_REF=HEAD \
PERF_MAIN_DRIVER_IMAGE=main@sha256:smoke \
PERF_BRANCH_DRIVER_IMAGE=branch@sha256:smoke \
PERF_MAIN_INSTALL_HOOK="${FIXTURES}/hook.sh" \
PERF_BRANCH_INSTALL_HOOK="${FIXTURES}/hook.sh" \
PERF_DECOMMISSION_HOOK="${FIXTURES}/hook.sh" \
PERF_TIER_C_RUNNER="${FIXTURES}/tier-c.sh" \
PERF_NODE_SELECTOR=scale-promotion.nvidia.com/tier-c=true \
  bash "${SCRIPT_DIR}/persistent-agent-two-node-performance.sh"

jq -e '.passed == true and .expectedBlocks == 1 and .expectedTrialsPerBlock == 5' \
  "${TMP_DIR}/artifacts/comparison/comparison.json" > /dev/null
test "$(wc -l < "${TMP_DIR}/artifacts/manifest.csv" | tr -d ' ')" = 5
test "$(wc -l < "${TMP_DIR}/artifacts/installations.csv" | tr -d ' ')" = 3
echo "PASS: two-Node performance orchestration smoke"
