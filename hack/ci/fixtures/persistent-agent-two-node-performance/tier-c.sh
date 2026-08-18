#!/usr/bin/env bash
# Synthetic Tier C producer used only by the orchestration smoke test.
set -o errexit
set -o nounset
set -o pipefail

: "${ARTIFACTS:?}"
: "${TIER_C_PROVIDER:?}"
: "${TIER_C_ARM:?}"
: "${TIER_C_SCENARIO:?}"
: "${TIER_C_TRIALS:?}"

base="${ARTIFACTS}/${TIER_C_PROVIDER}/1x2"
mkdir -p "${base}/aggregate" "${base}/measured"
node_prepare=1000
if [[ "${TIER_C_ARM}" == "B" && "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
  node_prepare=500
fi
jq -n \
  --arg arm "${TIER_C_ARM}" --arg scenario "${TIER_C_SCENARIO}" \
  --argjson trials "${TIER_C_TRIALS}" --argjson nodePrepare "${node_prepare}" \
  '{timelines:[range(1; $trials+1) as $trial | range(0; 2) as $pod |
    {trialID:("smoke-"+$arm+"-"+$scenario+"-"+($trial|tostring)),schedulerMS:100,nodePrepareMS:$nodePrepare,readinessMS:100,totalMS:($nodePrepare+200)}]}' \
  > "${base}/aggregate/result.json"

if [[ "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
  jq -nc --argjson trials "${TIER_C_TRIALS}" '
    range(1; $trials+1) | {trialID:("smoke-"+tostring),cycleClass:"measured",fenceMS:1000,finalizationMS:1200,reuseReadyMS:1300}' \
    > "${ARTIFACTS}/lifecycle.jsonl"
else
  printf '%s\n' '{"trialID":"retirement","cycleClass":"retirement","fenceMS":1000,"finalizationMS":1200,"reuseReadyMS":1300}' \
    > "${ARTIFACTS}/lifecycle.jsonl"
fi

for ((trial = 1; trial <= TIER_C_TRIALS; trial++)); do
  directory="${base}/measured/$(printf '%03d' "${trial}")"
  mkdir -p "${directory}"
  printf '%s\n' '{"items":[{"spec":{"nodeName":"node-a"},"status":{"containerStatuses":[{"imageID":"sha256:workload"}]}},{"spec":{"nodeName":"node-b"},"status":{"containerStatuses":[{"imageID":"sha256:workload"}]}}]}' > "${directory}/pods.json"
  if [[ "${TIER_C_ARM}" == "M" && "${TIER_C_SCENARIO}" == "cold-domain" ]]; then
    printf '%s\n' '{"type":"ADDED"}' > "${directory}/daemonset-watch.json"
  fi
done
