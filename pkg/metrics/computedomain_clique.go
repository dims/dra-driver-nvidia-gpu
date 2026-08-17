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
	"context"
	"errors"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	CliqueAPIResourceSnapshot      = "snapshot"
	CliqueAPIResourceReservation   = "reservation"
	CliqueAPIResourceEvidence      = "retirement_evidence"
	CliqueAPIResourceNode          = "node"
	CliqueAPIResourcePod           = "pod"
	CliqueAPIResourceComputeDomain = "compute_domain"

	CliqueAPIOperationCreate             = "create"
	CliqueAPIOperationDelete             = "delete"
	CliqueAPIOperationGet                = "get"
	CliqueAPIOperationWriteBarrierGet    = "write_barrier_get"
	CliqueAPIOperationFinalizerAdd       = "finalizer_add"
	CliqueAPIOperationFinalizerRemove    = "finalizer_remove"
	CliqueAPIOperationStatusUpdate       = "status_update"
	CliqueAPIOperationAttestationUpdate  = "attestation_update"
	CliqueAPIOperationAppliedStateUpdate = "applied_state_update"

	CliqueAPIResultSuccess       = "success"
	CliqueAPIResultAlreadyExists = "already_exists"
	CliqueAPIResultConflict      = "conflict"
	CliqueAPIResultThrottled     = "throttled"
	CliqueAPIResultNotFound      = "not_found"
	CliqueAPIResultForbidden     = "forbidden"
	CliqueAPIResultTimeout       = "timeout"
	CliqueAPIResultError         = "error"
)

func CliqueAPIResultForError(err error) string {
	switch {
	case err == nil:
		return CliqueAPIResultSuccess
	case apierrors.IsAlreadyExists(err):
		return CliqueAPIResultAlreadyExists
	case apierrors.IsNotFound(err):
		return CliqueAPIResultNotFound
	case apierrors.IsConflict(err):
		return CliqueAPIResultConflict
	case apierrors.IsTooManyRequests(err):
		return CliqueAPIResultThrottled
	case apierrors.IsForbidden(err):
		return CliqueAPIResultForbidden
	case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), errors.Is(err, context.DeadlineExceeded):
		return CliqueAPIResultTimeout
	default:
		return CliqueAPIResultError
	}
}

var (
	cliqueMetricsOnce sync.Once
	cliqueReconciles  = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_reconcile_total", Help: "Persistent-agent clique reconciles partitioned by bounded result."},
		[]string{"protocol", "result"},
	)
	cliqueReconcileDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{Namespace: "nvidia_dra", Name: "cdc_reconcile_duration_seconds", Help: "Persistent-agent clique reconcile duration.", Buckets: metrics.ExponentialBuckets(0.001, 2, 15)},
		[]string{"protocol"},
	)
	cliqueAPIActions = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_api_actions_total", Help: "Persistent-agent controller API calls, including successful no-mutation outcomes such as AlreadyExists."},
		[]string{"protocol", "resource", "operation", "result"},
	)
	cliqueWrites = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_api_writes_total", Help: "Confirmed persistent-agent controller API mutations. AlreadyExists and failed requests are not writes."},
		[]string{"protocol", "resource", "operation"},
	)
)

func registerCliqueMetrics() {
	cliqueMetricsOnce.Do(func() {
		legacyregistry.MustRegister(cliqueReconciles, cliqueReconcileDuration, cliqueAPIActions, cliqueWrites)
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
