#!/usr/bin/env bash
# Synthetic install/decommission hook used only by the orchestration smoke test.
set -o errexit
set -o nounset
set -o pipefail

: "${PERF_ACTION:?}"
: "${PERF_ARM:?}"
: "${PERF_SOURCE_SHA:?}"
printf '%s\n' "${PERF_ACTION} ${PERF_ARM} ${PERF_SOURCE_SHA}"
