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

# Runs the local persistent-agent scale gate. This exercises the real
# attestation and snapshot reconcile functions against informer caches and a
# fake API tracker. The byte metrics are serialized fixture payloads, not
# measurements from kube-apiserver, etcd, or a watch connection.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BENCH_COUNT="${BENCH_COUNT:-5}"
SHORT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short=12 HEAD)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${SHORT_SHA}"
ARTIFACTS="${ARTIFACTS:-/tmp/persistent-agent-scale/${RUN_ID}}"

for tool in go git tee uname; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done

mkdir -p "${ARTIFACTS}"

{
  echo "run_id=${RUN_ID}"
  echo "commit=$(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "branch=$(git -C "${REPO_ROOT}" branch --show-current)"
  echo "dirty_files=$(git -C "${REPO_ROOT}" status --porcelain=v1 | wc -l | tr -d ' ')"
  echo "go=$(go version)"
  echo "host=$(uname -a)"
  echo "gomaxprocs=${GOMAXPROCS:-default}"
} > "${ARTIFACTS}/environment.txt"

cat > "${ARTIFACTS}/contract.txt" <<'EOF'
The local formation contract is:

  actions = N + 6*C
  writes  = N + 5*C

N is the number of Nodes and C is the number of physical cliques. The extra
read action per clique validates the reservation before first publication.

Expected shapes:
  18 Nodes, 1 clique:       24 actions,   23 writes
  144 Nodes, 1 clique:     150 actions,  149 writes
  5,040 Nodes, 280 cliques: 6,720 actions, 6,440 writes

The fixture-request-bytes/op and fixture-watch-bytes/op benchmark metrics are
serialized object-size estimates. They are not production API, etcd, audit,
or watch-egress measurements and must not be promoted as such.
EOF

cd "${REPO_ROOT}"

go test -mod=vendor ./cmd/compute-domain-controller \
  -run '^TestPersistentAgentScaleHarnessAccountsForFormation$' -count=1 -v \
  2>&1 | tee "${ARTIFACTS}/unit.log"

go test -mod=vendor -race ./cmd/compute-domain-controller \
  -run '^TestPersistentAgentScaleHarnessAccountsForFormation$' -count=1 -v \
  2>&1 | tee "${ARTIFACTS}/race.log"

go test -mod=vendor ./cmd/compute-domain-controller -run '^$' \
  -bench '^(BenchmarkIndexedReconcileInputs(18|144|280x18)|BenchmarkAllocateSelectedNodes144|BenchmarkSnapshotHash144)$' \
  -benchmem -count="${BENCH_COUNT}" \
  2>&1 | tee "${ARTIFACTS}/microbenchmarks.log"

go test -mod=vendor ./cmd/compute-domain-controller -run '^$' \
  -bench '^BenchmarkPersistentAgentFormation(18|144|280x18)$' \
  -benchtime=1x -benchmem -count="${BENCH_COUNT}" \
  2>&1 | tee "${ARTIFACTS}/formation-benchmarks.log"

cat > "${ARTIFACTS}/README.md" <<EOF
# Persistent-agent local scale evidence

- Run: \`${RUN_ID}\`
- Commit: \`$(git rev-parse HEAD)\`
- Benchmark repetitions: \`${BENCH_COUNT}\`

This bundle is the deterministic local gate. It proves controller action/write
accounting, steady-state no-op behavior, race cleanliness for the small state
machine, and directional CPU/allocation behavior at 18, 144, and 280×18.

It does **not** prove real API-server latency or bytes, informer delivery,
scheduler convergence, NodePrepareResources, container readiness, IMEX, or
T0–T3. Those require the real-API and capable-node promotion tiers.
EOF

echo "PASS: persistent-agent local scale gate"
echo "Artifacts: ${ARTIFACTS}"
