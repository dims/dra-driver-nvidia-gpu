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

# Exercises controller-owned CDC Node admission against a real Kubernetes API
# server. Fake clients do not evaluate ValidatingAdmissionPolicy expressions.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-controller-owned-cdc-admission-$$}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
TEST_NAMESPACE="${TEST_NAMESPACE:-controller-owned-cdc-admission}"
RELEASE_NAME="${RELEASE_NAME:-admission-test}"
SKIP_CLEANUP="${SKIP_CLEANUP:-false}"

NODE_NAME="controller-owned-cdc-admission-node"
CD_UID="11111111-2222-3333-4444-555555555555"
ROUTE_KEY="resource.nvidia.com/computeDomain"
ISOLATION_KEY="resource.nvidia.com/controllerOwnedComputeDomain"
ATTESTATION_KEY="resource.nvidia.com/computeDomainAttestation"

for tool in kind kubectl helm rg; do
  if ! command -v "${tool}" > /dev/null 2>&1; then
    echo "ERROR: ${tool} is required" >&2
    exit 1
  fi
done

TMP_DIR="$(mktemp -d)"
cleanup() {
  local exit_code=$?
  if [ "${SKIP_CLEANUP}" != "true" ]; then
    kind delete cluster --name "${KIND_CLUSTER_NAME}" > /dev/null 2>&1 || true
  else
    echo "Keeping Kind cluster ${KIND_CLUSTER_NAME}"
  fi
  rm -rf "${TMP_DIR}"
  exit "${exit_code}"
}
trap cleanup EXIT

kind create cluster --name "${KIND_CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}" --wait 120s
kubectl create namespace "${TEST_NAMESPACE}"

HELM_ARGS=(
  "${RELEASE_NAME}"
  "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu"
  --namespace "${TEST_NAMESPACE}"
  --api-versions resource.k8s.io/v1
  --set resources.gpus.enabled=false
  --set controllerOwnedCDCliques.admissionEnabled=true
)

helm template "${HELM_ARGS[@]}" \
  --show-only templates/controllerownedcdc-installation.yaml \
  > "${TMP_DIR}/installation.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/rbac-controller.yaml \
  > "${TMP_DIR}/controller-rbac.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/validatingadmissionpolicy.yaml \
  > "${TMP_DIR}/policies.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/validatingadmissionpolicybinding.yaml \
  > "${TMP_DIR}/bindings.yaml"

# Install authorization and policy parameters before activating the bindings.
kubectl apply -f "${TMP_DIR}/installation.yaml"
kubectl apply -f "${TMP_DIR}/controller-rbac.yaml"

INSTALLATION_NAME="controller-owned-cdc-installation.dra-driver-nvidia-gpu"
CONTROLLER_SUBJECT="$(kubectl get clusterrole "${INSTALLATION_NAME}" \
  -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/controller-owned-cdc-controller-subject}')"
test "$(kubectl auth can-i create events -n "${TEST_NAMESPACE}" --as="${CONTROLLER_SUBJECT}")" = "yes"
echo "PASS: controller can create operator-action Events"

# JSON Patch avoids resource-version races with the Node lifecycle controller.
# The production controller uses Update; its exact object construction remains
# covered by nodeattestation_test.go. This role exists only in this disposable
# test cluster and lets both identities submit the same admission transition.
kubectl apply -f - > /dev/null <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: controller-owned-cdc-admission-test-node-patcher
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: controller-owned-cdc-admission-test-node-patcher
subjects:
- kind: User
  name: admission-test-operator
  apiGroup: rbac.authorization.k8s.io
- kind: User
  name: ${CONTROLLER_SUBJECT}
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: controller-owned-cdc-admission-test-node-patcher
  apiGroup: rbac.authorization.k8s.io
EOF

kubectl apply -f "${TMP_DIR}/policies.yaml"
kubectl apply -f "${TMP_DIR}/bindings.yaml"

create_attested_node() {
  kubectl delete node "${NODE_NAME}" --ignore-not-found > /dev/null
  kubectl create -f - > /dev/null <<EOF
apiVersion: v1
kind: Node
metadata:
  name: ${NODE_NAME}
  labels:
    ${ROUTE_KEY}: "${CD_UID}"
    ${ISOLATION_KEY}: "${CD_UID}"
    admission-test-preserved-label: preserved
  annotations:
    ${ATTESTATION_KEY}: '{"computeDomainUID":"${CD_UID}"}'
    admission-test-preserved-annotation: preserved
spec: {}
EOF
}

patch_node() {
  local actor=$1
  local mode=$2

  case "${mode}" in
    atomic-clear)
      kubectl --as="${actor}" patch node "${NODE_NAME}" --type=json --patch="[
        {\"op\":\"remove\",\"path\":\"/metadata/labels/resource.nvidia.com~1computeDomain\"},
        {\"op\":\"remove\",\"path\":\"/metadata/labels/resource.nvidia.com~1controllerOwnedComputeDomain\"},
        {\"op\":\"remove\",\"path\":\"/metadata/annotations/resource.nvidia.com~1computeDomainAttestation\"},
        {\"op\":\"add\",\"path\":\"/metadata/annotations/resource.nvidia.com~1computeDomainCliqueRetirementFenced\",\"value\":\"${CD_UID}\"}
      ]"
      ;;
    isolation-only)
      kubectl --as="${actor}" patch node "${NODE_NAME}" --type=json --patch="[
        {\"op\":\"remove\",\"path\":\"/metadata/labels/resource.nvidia.com~1controllerOwnedComputeDomain\"}
      ]"
      ;;
    unrelated-change)
      kubectl --as="${actor}" patch node "${NODE_NAME}" --type=json --patch='[
        {"op":"replace","path":"/metadata/labels/admission-test-preserved-label","value":"changed"}
      ]'
      ;;
    *)
      echo "ERROR: unknown replacement mode ${mode}" >&2
      return 1
      ;;
  esac
}

expect_denied() {
  local description=$1
  shift
  local output
  if output=$("$@" 2>&1); then
    echo "ERROR: ${description} unexpectedly succeeded" >&2
    exit 1
  fi
  if ! rg -q 'ValidatingAdmissionPolicy|denied request' <<<"${output}"; then
    echo "ERROR: ${description} failed outside admission: ${output}" >&2
    exit 1
  fi
  echo "PASS: ${description}"
}

wait_for_admission() {
  local attempt output
  for ((attempt = 1; attempt <= 30; attempt++)); do
    create_attested_node
    if output=$(patch_node "admission-test-operator" atomic-clear 2>&1); then
      sleep 0.5
      continue
    fi
    if rg -q 'ValidatingAdmissionPolicy|denied request' <<<"${output}"; then
      echo "Admission policy active after ${attempt} probe(s)"
      return
    fi
    echo "ERROR: admission readiness probe failed outside admission: ${output}" >&2
    exit 1
  done
  echo "ERROR: admission policy did not become active" >&2
  exit 1
}

wait_for_admission

create_attested_node
expect_denied "non-controller atomic cleanup" \
  patch_node "admission-test-operator" atomic-clear

create_attested_node
expect_denied "controller isolation-only cleanup" \
  patch_node "${CONTROLLER_SUBJECT}" isolation-only

create_attested_node
expect_denied "controller unrelated metadata mutation" \
  patch_node "${CONTROLLER_SUBJECT}" unrelated-change

create_attested_node
patch_node "${CONTROLLER_SUBJECT}" atomic-clear > /dev/null

test -z "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.labels.resource\.nvidia\.com/computeDomain}')"
test -z "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.labels.resource\.nvidia\.com/controllerOwnedComputeDomain}')"
test -z "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/computeDomainAttestation}')"
test "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/computeDomainCliqueRetirementFenced}')" = "${CD_UID}"
test "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.labels.admission-test-preserved-label}')" = preserved
test "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.annotations.admission-test-preserved-annotation}')" = preserved

echo "PASS: controller atomic retirement cleanup"
