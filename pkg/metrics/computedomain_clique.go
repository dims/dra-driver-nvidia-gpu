/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	CliqueAPIResourceSnapshot    = "snapshot"
	CliqueAPIResourceReservation = "reservation"
	CliqueAPIResourceNode        = "node"

	CliqueAPIOperationCreate            = "create"
	CliqueAPIOperationDelete            = "delete"
	CliqueAPIOperationGet               = "get"
	CliqueAPIOperationWriteBarrierGet   = "write_barrier_get"
	CliqueAPIOperationFinalizerAdd      = "finalizer_add"
	CliqueAPIOperationFinalizerRemove   = "finalizer_remove"
	CliqueAPIOperationStatusUpdate      = "status_update"
	CliqueAPIOperationAttestationUpdate = "attestation_update"

	CliqueAPIResultSuccess       = "success"
	CliqueAPIResultAlreadyExists = "already_exists"
	CliqueAPIResultConflict      = "conflict"
	CliqueAPIResultThrottled     = "throttled"
	CliqueAPIResultNotFound      = "not_found"
	CliqueAPIResultForbidden     = "forbidden"
	CliqueAPIResultTimeout       = "timeout"
	CliqueAPIResultError         = "error"

	// T0 through T3 use common externally observable workload milestones. They
	// deliberately do not start at the final eligible daemon Pod because that
	// would exclude scheduler and DaemonSet propagation from the comparison.
	CliqueMilestoneT0WorkloadAccepted  = "t0_workload_accepted"
	CliqueMilestoneT1WorkloadScheduled = "t1_workload_scheduled"
	CliqueMilestoneT2PrepareComplete   = "t2_prepare_complete"
	CliqueMilestoneT3WorkloadReady     = "t3_workload_ready"

	CliqueStageT1MinusT0 = "t1_minus_t0"
	CliqueStageT2MinusT1 = "t2_minus_t1"
	CliqueStageT3MinusT2 = "t3_minus_t2"
	CliqueStageT3MinusT0 = "t3_minus_t0"
	CliqueStageT3MinusT1 = "t3_minus_t1"
)

// CliqueWorkloadTimeline is one end-to-end sample. Callers are expected to
// derive these timestamps from a common experiment trace; no component should
// invent a missing milestone from local controller time.
type CliqueWorkloadTimeline struct {
	T0WorkloadAccepted  time.Time
	T1WorkloadScheduled time.Time
	T2PrepareComplete   time.Time
	T3WorkloadReady     time.Time
}

var (
	cliqueMetricsOnce sync.Once
	cliqueReconciles  = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_reconcile_total", Help: "Controller-owned clique reconciles partitioned by bounded result."},
		[]string{"protocol", "result"},
	)
	cliqueReconcileDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{Namespace: "nvidia_dra", Name: "cdc_reconcile_duration_seconds", Help: "Controller-owned clique reconcile duration.", Buckets: metrics.ExponentialBuckets(0.001, 2, 15)},
		[]string{"protocol"},
	)
	cliqueAPIActions = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_api_actions_total", Help: "Controller-owned clique API calls, including successful no-mutation outcomes such as AlreadyExists."},
		[]string{"protocol", "resource", "operation", "result"},
	)
	cliqueWrites = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_api_writes_total", Help: "Confirmed controller-owned clique API mutations. AlreadyExists and failed requests are not writes."},
		[]string{"protocol", "resource", "operation"},
	)
	cliqueWorkloadStageDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{Namespace: "nvidia_dra", Name: "cdc_workload_stage_duration_seconds", Help: "End-to-end controller-owned clique workload stage durations using the documented T0 through T3 milestones.", Buckets: metrics.ExponentialBuckets(0.001, 2, 18)},
		[]string{"protocol", "stage"},
	)
)

func registerCliqueMetrics() {
	cliqueMetricsOnce.Do(func() {
		legacyregistry.MustRegister(cliqueReconciles, cliqueReconcileDuration, cliqueAPIActions, cliqueWrites, cliqueWorkloadStageDuration)
	})
}

func ObserveCliqueReconcile(protocol, result string, duration time.Duration) {
	registerCliqueMetrics()
	cliqueReconciles.WithLabelValues(protocol, result).Inc()
	cliqueReconcileDuration.WithLabelValues(protocol).Observe(duration.Seconds())
}

// ObserveCliqueAPIAction records every explicit API call made by the clique
// state machine. mutated must be true only when kube-apiserver confirmed that
// this call changed persistent state. In particular, AlreadyExists is a
// successful idempotency outcome but not a write.
func ObserveCliqueAPIAction(protocol, resource, operation, result string, mutated bool) {
	registerCliqueMetrics()
	cliqueAPIActions.WithLabelValues(protocol, resource, operation, result).Inc()
	if mutated && result == CliqueAPIResultSuccess {
		cliqueWrites.WithLabelValues(protocol, resource, operation).Inc()
	}
}

// ObserveCliqueWorkloadTimeline validates and records one T0-T3 sample.
func ObserveCliqueWorkloadTimeline(protocol string, timeline CliqueWorkloadTimeline) error {
	milestones := []struct {
		name string
		at   time.Time
	}{
		{name: CliqueMilestoneT0WorkloadAccepted, at: timeline.T0WorkloadAccepted},
		{name: CliqueMilestoneT1WorkloadScheduled, at: timeline.T1WorkloadScheduled},
		{name: CliqueMilestoneT2PrepareComplete, at: timeline.T2PrepareComplete},
		{name: CliqueMilestoneT3WorkloadReady, at: timeline.T3WorkloadReady},
	}
	for i := range milestones {
		if milestones[i].at.IsZero() {
			return fmt.Errorf("clique workload milestone %s is missing", milestones[i].name)
		}
		if i > 0 && milestones[i].at.Before(milestones[i-1].at) {
			return fmt.Errorf("clique workload milestone %s precedes %s", milestones[i].name, milestones[i-1].name)
		}
	}

	registerCliqueMetrics()
	observations := []struct {
		stage    string
		duration time.Duration
	}{
		{stage: CliqueStageT1MinusT0, duration: timeline.T1WorkloadScheduled.Sub(timeline.T0WorkloadAccepted)},
		{stage: CliqueStageT2MinusT1, duration: timeline.T2PrepareComplete.Sub(timeline.T1WorkloadScheduled)},
		{stage: CliqueStageT3MinusT2, duration: timeline.T3WorkloadReady.Sub(timeline.T2PrepareComplete)},
		{stage: CliqueStageT3MinusT0, duration: timeline.T3WorkloadReady.Sub(timeline.T0WorkloadAccepted)},
		{stage: CliqueStageT3MinusT1, duration: timeline.T3WorkloadReady.Sub(timeline.T1WorkloadScheduled)},
	}
	for _, observation := range observations {
		cliqueWorkloadStageDuration.WithLabelValues(protocol, observation.stage).Observe(observation.duration.Seconds())
	}
	return nil
}
