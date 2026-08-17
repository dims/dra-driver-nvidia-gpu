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

# Exercises persistent-agent CDC Node admission against a real Kubernetes API
# server. Fake clients do not evaluate ValidatingAdmissionPolicy expressions.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-persistent-agent-admission-$$}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
TEST_NAMESPACE="${TEST_NAMESPACE:-persistent-agent-admission}"
RELEASE_NAME="${RELEASE_NAME:-admission-test}"
SKIP_CLEANUP="${SKIP_CLEANUP:-false}"

NODE_NAME="persistent-agent-admission-node"
CD_UID="11111111-2222-3333-4444-555555555555"
ROUTE_KEY="resource.nvidia.com/computeDomain"
ISOLATION_KEY="resource.nvidia.com/persistentAgentComputeDomain"
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

helm template "${RELEASE_NAME}" "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu" \
  --namespace "${TEST_NAMESPACE}" --api-versions resource.k8s.io/v1 \
  --set resources.gpus.enabled=false --show-only templates/validatingadmissionpolicy.yaml \
  | kubectl apply --dry-run=server -f - > /dev/null
echo "PASS: legacy-only ResourceSlice policy compiles"

# The default controller performs a read-only durable-state probe even when
# persistent admission is disabled. Prove the ordinary chart grants exactly
# that read and no persistent mutation authority.
kubectl apply -f "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu/crds/resource.nvidia.com_computedomaincliquereservations.yaml" > /dev/null
kubectl wait --for=condition=Established crd/computedomaincliquereservations.resource.nvidia.com > /dev/null
helm template "${RELEASE_NAME}" "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu" \
  --namespace "${TEST_NAMESPACE}" --api-versions resource.k8s.io/v1 \
  --set resources.gpus.enabled=false \
  --show-only templates/rbac-controller.yaml \
  > "${TMP_DIR}/default-controller-rbac.yaml"
kubectl apply -f "${TMP_DIR}/default-controller-rbac.yaml" > /dev/null
DEFAULT_CONTROLLER_SUBJECT="system:serviceaccount:${TEST_NAMESPACE}:${RELEASE_NAME}-dra-driver-nvidia-gpu-service-account-controller"
test "$(kubectl auth can-i list computedomaincliquereservations.resource.nvidia.com --as="${DEFAULT_CONTROLLER_SUBJECT}")" = "yes"
test "$(kubectl auth can-i create computedomaincliquereservations.resource.nvidia.com --as="${DEFAULT_CONTROLLER_SUBJECT}")" = "no"
kubectl delete -f "${TMP_DIR}/default-controller-rbac.yaml" > /dev/null
echo "PASS: default controller can probe reservations without persistent write authority"

# The legacy kubelet path performs the same read-only reservation check before
# it forms a per-domain daemon. Keep that read available in a plain install,
# without granting reservation mutation authority.
helm template "${RELEASE_NAME}" "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu" \
  --namespace "${TEST_NAMESPACE}" --api-versions resource.k8s.io/v1 \
  --set resources.gpus.enabled=false \
  --show-only templates/rbac-kubeletplugin.yaml \
  > "${TMP_DIR}/default-kubelet-rbac.yaml"
kubectl apply -f "${TMP_DIR}/default-kubelet-rbac.yaml" > /dev/null
DEFAULT_KUBELET_SUBJECT="system:serviceaccount:${TEST_NAMESPACE}:${RELEASE_NAME}-dra-driver-nvidia-gpu-service-account-kubeletplugin"
test "$(kubectl auth can-i get computedomaincliquereservations.resource.nvidia.com --as="${DEFAULT_KUBELET_SUBJECT}")" = "yes"
test "$(kubectl auth can-i create computedomaincliquereservations.resource.nvidia.com --as="${DEFAULT_KUBELET_SUBJECT}")" = "no"
kubectl delete -f "${TMP_DIR}/default-kubelet-rbac.yaml" > /dev/null
echo "PASS: default kubelet can check reservations without persistent write authority"

HELM_ARGS=(
  "${RELEASE_NAME}"
  "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu"
  --namespace "${TEST_NAMESPACE}"
  --api-versions resource.k8s.io/v1
  --set resources.gpus.enabled=false
  --set persistentComputeDomainAgents.admissionEnabled=true
)

helm template "${HELM_ARGS[@]}" \
  --show-only templates/persistent-agent-installation.yaml \
  > "${TMP_DIR}/installation.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/rbac-controller.yaml \
  > "${TMP_DIR}/controller-rbac.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/rbac-kubeletplugin.yaml \
  > "${TMP_DIR}/kubelet-rbac.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/rbac-compute-domain-daemon.yaml \
  > "${TMP_DIR}/daemon-rbac.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/validatingadmissionpolicy.yaml \
  > "${TMP_DIR}/policies.yaml"
helm template "${HELM_ARGS[@]}" \
  --show-only templates/validatingadmissionpolicybinding.yaml \
  > "${TMP_DIR}/bindings.yaml"

# Install authorization and policy parameters before activating the bindings.
kubectl apply -f "${TMP_DIR}/installation.yaml"
kubectl apply -f "${TMP_DIR}/controller-rbac.yaml"
kubectl apply -f "${TMP_DIR}/kubelet-rbac.yaml"
kubectl apply -f "${TMP_DIR}/daemon-rbac.yaml"

INSTALLATION_NAME="persistent-agent-installation.dra-driver-nvidia-gpu"
CONTROLLER_SUBJECT="$(kubectl get clusterrole "${INSTALLATION_NAME}" \
  -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/persistent-agent-controller-subject}')"
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
  name: persistent-agent-admission-test-node-patcher
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: persistent-agent-admission-test-node-patcher
subjects:
- kind: User
  name: admission-test-operator
  apiGroup: rbac.authorization.k8s.io
- kind: User
  name: ${CONTROLLER_SUBJECT}
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: persistent-agent-admission-test-node-patcher
  apiGroup: rbac.authorization.k8s.io
EOF

kubectl apply -f "${TMP_DIR}/policies.yaml"
kubectl apply -f "${TMP_DIR}/bindings.yaml"
kubectl apply -f "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu/crds"

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
        {\"op\":\"remove\",\"path\":\"/metadata/labels/resource.nvidia.com~1persistentAgentComputeDomain\"},
        {\"op\":\"remove\",\"path\":\"/metadata/annotations/resource.nvidia.com~1computeDomainAttestation\"},
        {\"op\":\"add\",\"path\":\"/metadata/annotations/resource.nvidia.com~1computeDomainCliqueRetirementFenced\",\"value\":\"${CD_UID}\"}
      ]"
      ;;
    isolation-only)
      kubectl --as="${actor}" patch node "${NODE_NAME}" --type=json --patch="[
        {\"op\":\"remove\",\"path\":\"/metadata/labels/resource.nvidia.com~1persistentAgentComputeDomain\"}
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

# A direct, offline render is non-transactional: kubectl may create harmless
# namespaced objects before the immutable installation marker rejects the
# second release. The safety property is that protected bindings/workloads are
# denied, the first release remains authorized, and alternate identities gain
# no cluster privilege.
SECOND_NAMESPACE="${TEST_NAMESPACE}-second"
kubectl create namespace "${SECOND_NAMESPACE}" > /dev/null
helm template second-install "${REPO_ROOT}/deployments/helm/dra-driver-nvidia-gpu" \
  --namespace "${SECOND_NAMESPACE}" --api-versions resource.k8s.io/v1 \
  --set resources.gpus.enabled=false \
  --set persistentComputeDomainAgents.admissionEnabled=true \
  --set nameOverride=second-driver \
  > "${TMP_DIR}/second-install.yaml"
expect_denied "offline second installation" \
  kubectl apply -f "${TMP_DIR}/second-install.yaml"

SECOND_KUBELET_SUBJECT="system:serviceaccount:${SECOND_NAMESPACE}:second-driver-service-account-kubeletplugin"
SECOND_CONTROLLER_SUBJECT="system:serviceaccount:${SECOND_NAMESPACE}:second-driver-service-account-controller"
test "$(kubectl auth can-i update nodes --as="${SECOND_KUBELET_SUBJECT}")" = "no"
test "$(kubectl auth can-i create resourceslices.resource.k8s.io --as="${SECOND_KUBELET_SUBJECT}")" = "no"
test "$(kubectl auth can-i create computedomaincliquereservations.resource.nvidia.com --as="${SECOND_CONTROLLER_SUBJECT}")" = "no"

FIRST_KUBELET_SUBJECT="$(kubectl get clusterrole "${INSTALLATION_NAME}" \
  -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/persistent-agent-kubelet-subject}')"
FIRST_KUBELET_BINDING="$(kubectl get clusterrole "${INSTALLATION_NAME}" \
  -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/persistent-agent-kubelet-binding}')"
FIRST_KUBELET_NAMESPACE="$(kubectl get clusterrolebinding "${FIRST_KUBELET_BINDING}" -o jsonpath='{.subjects[0].namespace}')"
FIRST_KUBELET_NAME="$(kubectl get clusterrolebinding "${FIRST_KUBELET_BINDING}" -o jsonpath='{.subjects[0].name}')"
test "${FIRST_KUBELET_SUBJECT}" = "system:serviceaccount:${FIRST_KUBELET_NAMESPACE}:${FIRST_KUBELET_NAME}"
test "$(kubectl auth can-i create events -n "${TEST_NAMESPACE}" --as="${CONTROLLER_SUBJECT}")" = "yes"
echo "PASS: second installation denied without rebinding cluster privilege"

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
test -z "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.labels.resource\.nvidia\.com/persistentAgentComputeDomain}')"
test -z "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/computeDomainAttestation}')"
test "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/computeDomainCliqueRetirementFenced}')" = "${CD_UID}"
test "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.labels.admission-test-preserved-label}')" = preserved
test "$(kubectl get node "${NODE_NAME}" -o jsonpath='{.metadata.annotations.admission-test-preserved-annotation}')" = preserved

echo "PASS: controller atomic retirement cleanup"

# Exercise the persistent agent's only write permission with a real
# Pod-bound ServiceAccount token. This proves the token UID/Node checks and
# narrow metadata diff against an API server, which fake clients cannot do.
# Remove the API-only Node used above; it has no kubelet and must not count as
# a schedulable DaemonSet target.
kubectl delete node "${NODE_NAME}" > /dev/null
kubectl create -f - > /dev/null <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: dra-driver-nvidia-gpu-persistent-agent
  namespace: ${TEST_NAMESPACE}
spec:
  selector:
    matchLabels:
      resource.nvidia.com/persistentComputeDomainAgent: "true"
  template:
    metadata:
      labels:
        resource.nvidia.com/persistentComputeDomainAgent: "true"
    spec:
      serviceAccountName: compute-domain-daemon-reader-service-account
      tolerations:
      - operator: Exists
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
EOF
if ! kubectl rollout status daemonset/dra-driver-nvidia-gpu-persistent-agent -n "${TEST_NAMESPACE}" --timeout=120s > /dev/null; then
  kubectl get pods -n "${TEST_NAMESPACE}" -o wide >&2
  kubectl describe daemonset/dra-driver-nvidia-gpu-persistent-agent -n "${TEST_NAMESPACE}" >&2
  exit 1
fi
AGENT_POD="$(kubectl get pods -n "${TEST_NAMESPACE}" -l resource.nvidia.com/persistentComputeDomainAgent=true -o jsonpath='{.items[0].metadata.name}')"
test -n "${AGENT_POD}"
AGENT_POD_UID="$(kubectl get pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" -o jsonpath='{.metadata.uid}')"
AGENT_NODE_NAME="$(kubectl get pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
test -n "${AGENT_POD_UID}"
test -n "${AGENT_NODE_NAME}"
test "$(kubectl get pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" -o jsonpath='{.metadata.ownerReferences[0].kind}')" = "DaemonSet"
test "$(kubectl get pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" -o jsonpath='{.metadata.ownerReferences[0].name}')" = "dra-driver-nvidia-gpu-persistent-agent"
kubectl create -f - > /dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: persistent-agent-other
  namespace: ${TEST_NAMESPACE}
spec:
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
EOF
AGENT_KUBECONFIG="${TMP_DIR}/persistent-agent.kubeconfig"
APISERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
kubectl config set-cluster admission-test --server="${APISERVER}" --insecure-skip-tls-verify=true --kubeconfig="${AGENT_KUBECONFIG}" > /dev/null
kubectl config set-context persistent-agent --cluster=admission-test --user=persistent-agent --namespace="${TEST_NAMESPACE}" --kubeconfig="${AGENT_KUBECONFIG}" > /dev/null
kubectl config use-context persistent-agent --kubeconfig="${AGENT_KUBECONFIG}" > /dev/null
AGENT_KUBECTL=(kubectl --kubeconfig="${AGENT_KUBECONFIG}")
for _ in $(seq 1 30); do
  AGENT_TOKEN="$(kubectl create token compute-domain-daemon-reader-service-account -n "${TEST_NAMESPACE}" \
    --bound-object-kind Pod --bound-object-name "${AGENT_POD}" --duration=10m)"
  kubectl config set-credentials persistent-agent --token="${AGENT_TOKEN}" --kubeconfig="${AGENT_KUBECONFIG}" > /dev/null
  TOKEN_POD_UID="$("${AGENT_KUBECTL[@]}" auth whoami -o jsonpath='{.status.userInfo.extra.authentication\.kubernetes\.io/pod-uid[0]}')"
  TOKEN_NODE_NAME="$("${AGENT_KUBECTL[@]}" auth whoami -o jsonpath='{.status.userInfo.extra.authentication\.kubernetes\.io/node-name[0]}')"
  if [ "${TOKEN_POD_UID}" = "${AGENT_POD_UID}" ] && [ "${TOKEN_NODE_NAME}" = "${AGENT_NODE_NAME}" ]; then
    break
  fi
  sleep 1
done
test "$("${AGENT_KUBECTL[@]}" auth whoami -o jsonpath='{.status.userInfo.username}')" = \
  "system:serviceaccount:${TEST_NAMESPACE}:compute-domain-daemon-reader-service-account"
test "${TOKEN_POD_UID}" = "${AGENT_POD_UID}"
test "${TOKEN_NODE_NAME}" = "${AGENT_NODE_NAME}"

"${AGENT_KUBECTL[@]}" patch pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" --type=merge \
  -p '{"metadata":{"annotations":{"resource.nvidia.com/computeDomainCliqueSnapshotApplied":"{\"snapshotUID\":\"snapshot-a\"}"}}}' > /dev/null
test "$(kubectl get pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" -o jsonpath='{.metadata.annotations.resource\.nvidia\.com/computeDomainCliqueSnapshotApplied}')" = '{"snapshotUID":"snapshot-a"}'

expect_denied "persistent agent unrelated Pod annotation mutation" \
  "${AGENT_KUBECTL[@]}" patch pod "${AGENT_POD}" -n "${TEST_NAMESPACE}" --type=merge \
    -p '{"metadata":{"annotations":{"admission-test.invalid/unrelated":"changed"}}}'
expect_denied "persistent agent mutation of a different Pod" \
  "${AGENT_KUBECTL[@]}" patch pod persistent-agent-other -n "${TEST_NAMESPACE}" --type=merge \
    -p '{"metadata":{"annotations":{"resource.nvidia.com/computeDomainCliqueSnapshotApplied":"{}"}}}'

echo "PASS: Pod-bound persistent-agent applied-state update"
