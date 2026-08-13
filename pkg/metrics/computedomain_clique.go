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
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

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
	cliqueWrites = metrics.NewCounterVec(
		&metrics.CounterOpts{Namespace: "nvidia_dra", Name: "cdc_api_writes_total", Help: "Controller-owned clique API mutations partitioned by operation and result."},
		[]string{"protocol", "operation", "result"},
	)
)

func registerCliqueMetrics() {
	cliqueMetricsOnce.Do(func() {
		legacyregistry.MustRegister(cliqueReconciles, cliqueReconcileDuration, cliqueWrites)
	})
}

func ObserveCliqueReconcile(protocol, result string, duration time.Duration) {
	registerCliqueMetrics()
	cliqueReconciles.WithLabelValues(protocol, result).Inc()
	cliqueReconcileDuration.WithLabelValues(protocol).Observe(duration.Seconds())
}

func ObserveCliqueWrite(protocol, operation, result string) {
	registerCliqueMetrics()
	cliqueWrites.WithLabelValues(protocol, operation, result).Inc()
}
