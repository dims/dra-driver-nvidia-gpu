# shellcheck disable=SC2148
# shellcheck disable=SC2329

setup_file() {
  load 'helpers.sh'
  _common_setup
  local _iargs=("--set" "logVerbosity=6")
  if [ "${DISABLE_COMPUTE_DOMAINS:-}" = "true" ]; then
    _iargs+=("--set" "resources.computeDomains.enabled=false")
  fi
  iupgrade_wait "${TEST_CHART_REPO}" "${TEST_CHART_VERSION}" _iargs
}

setup() {
  load 'helpers.sh'
  _common_setup
  log_objects
}

bats::on_failure() {
  echo -e "\n\nFAILURE HOOK START"
  log_objects
  show_kubelet_plugin_error_logs
  show_gpu_plugin_log_tails
  echo -e "FAILURE HOOK END\n\n"
}


# bats test_tags=fastfeedback,gpu-workloads
@test "GPUs: single GPU runs CUDA demo suite (deviceQuery, vectorAdd, bandwidthTest)" {
  local _specpath="tests/bats/specs/gpu-cuda-demo-suite.yaml"
  local _podname="pod-cuda-demo"

  kubectl apply -f "${_specpath}"
  # Longer timeout — installs cuda-demo-suite package at runtime.
  # arm64 (GH200) apt mirrors can be significantly slower than amd64.
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pods "${_podname}" --timeout=900s

  run kubectl logs "${_podname}"
  assert_output --partial "NVIDIA-SMI"
  assert_output --partial "Driver Version:"
  assert_output --partial "CUDA Version:"
  assert_output --partial "deviceQuery, CUDA Driver"
  assert_output --partial "Result = PASS"

  kubectl delete -f "${_specpath}"
  kubectl wait --for=delete pods "${_podname}" --timeout=30s
}


# bats test_tags=fastfeedback,gpu-workloads
@test "GPUs: Job with ResourceClaimTemplate allocates GPUs to completions" {
  local _specpath="tests/bats/specs/gpu-job-rct.yaml"
  local _jobname="gpu-job-rct"

  kubectl apply -f "${_specpath}"

  # Wait for job to complete (2 completions, parallelism 1 — safe for single GPU)
  kubectl wait --for=condition=Complete "job/${_jobname}" --timeout=600s

  # Verify both completions succeeded
  local succeeded
  succeeded=$(kubectl get job "${_jobname}" -o jsonpath='{.status.succeeded}')
  [ "${succeeded}" -eq 2 ]

  local failed
  failed=$(kubectl get job "${_jobname}" -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
  [ "${failed:-0}" -eq 0 ]

  # Verify each pod saw a GPU
  for pod in $(kubectl get pods -l "job-name=${_jobname}" -o name); do
    run kubectl logs "${pod}"
    assert_output --partial "UUID: GPU-"
  done

  kubectl delete -f "${_specpath}"
  kubectl wait --for=delete "job/${_jobname}" --timeout=30s
}


# bats test_tags=gpu-workloads,multi-gpu
@test "GPUs: Deployment with 2 replicas gets distinct GPUs simultaneously" {
  local _specpath="tests/bats/specs/gpu-job-rct-parallel.yaml"
  local _deployname="gpu-deploy-parallel"

  kubectl apply -f "${_specpath}"

  # Deployment Available condition proves 2 replicas are Running simultaneously,
  # each holding a distinct GPU via its own ResourceClaimTemplate instance.
  kubectl wait --for=condition=Available "deployment/${_deployname}" --timeout=60s

  # Verify 2 ready replicas
  local ready
  ready=$(kubectl get deployment "${_deployname}" -o jsonpath='{.status.readyReplicas}')
  [ "${ready}" -eq 2 ]

  # Collect GPU UUIDs from each pod and verify they are different
  local -a uuids=()
  for pod in $(kubectl get pods -l "app=${_deployname}" -o name); do
    local logs
    logs=$(kubectl logs "${pod}")
    echo "${logs}" | grep -q "UUID: GPU-"
    local uid
    uid=$(echo "${logs}" | grep -o "GPU-[a-f0-9-]*")
    uuids+=("${uid}")
  done
  [ "${#uuids[@]}" -eq 2 ]
  assert_not_equal "${uuids[0]}" "${uuids[1]}"

  kubectl delete -f "${_specpath}"
  kubectl wait --for=delete pods -l "app=${_deployname}" --timeout=30s
}


# bats test_tags=gpu-workloads,gpu-busgrind
@test "GPUs: busGrind memory bandwidth stress test" {
  local _specpath="tests/bats/specs/gpu-cuda-busgrind.yaml"
  local _podname="pod-cuda-busgrind"

  kubectl apply -f "${_specpath}"
  # busGrind can take several minutes on slower GPUs
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pods "${_podname}" --timeout=900s

  run kubectl logs "${_podname}"
  assert_output --partial "NVIDIA-SMI"
  assert_output --partial "busGrind"

  kubectl delete -f "${_specpath}"
  kubectl wait --for=delete pods "${_podname}" --timeout=30s
}
