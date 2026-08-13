/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func resetCliqueMetricsForTest() {
	registerCliqueMetrics()
	cliqueReconciles.Reset()
	cliqueReconcileDuration.Reset()
	cliqueAPIActions.Reset()
	cliqueWrites.Reset()
	cliqueWorkloadStageDuration.Reset()
}

func scrapeCliqueMetrics(t *testing.T) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	NewLegacyPrometheusHandler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	return rec.Body.String()
}

func TestCliqueAPIActionsSeparateAttemptsFromConfirmedWrites(t *testing.T) {
	resetCliqueMetricsForTest()
	t.Cleanup(resetCliqueMetricsForTest)

	const protocol = "controller-v1"
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationCreate, CliqueAPIResultSuccess, true)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationWriteBarrierGet, CliqueAPIResultSuccess, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceReservation, CliqueAPIOperationCreate, CliqueAPIResultSuccess, true)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceReservation, CliqueAPIOperationCreate, CliqueAPIResultAlreadyExists, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceReservation, CliqueAPIOperationGet, CliqueAPIResultSuccess, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationFinalizerAdd, CliqueAPIResultSuccess, true)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationFinalizerRemove, CliqueAPIResultSuccess, true)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationStatusUpdate, CliqueAPIResultSuccess, true)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationStatusUpdate, CliqueAPIResultError, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationStatusUpdate, CliqueAPIResultConflict, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationStatusUpdate, CliqueAPIResultThrottled, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationStatusUpdate, CliqueAPIResultForbidden, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationStatusUpdate, CliqueAPIResultTimeout, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationDelete, CliqueAPIResultSuccess, true)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceSnapshot, CliqueAPIOperationDelete, CliqueAPIResultNotFound, false)
	ObserveCliqueAPIAction(protocol, CliqueAPIResourceNode, CliqueAPIOperationAttestationUpdate, CliqueAPIResultSuccess, true)

	body := scrapeCliqueMetrics(t)
	for _, expected := range []string{
		`nvidia_dra_cdc_api_actions_total{operation="create",protocol="controller-v1",resource="reservation",result="success"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="create",protocol="controller-v1",resource="reservation",result="already_exists"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="get",protocol="controller-v1",resource="reservation",result="success"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="write_barrier_get",protocol="controller-v1",resource="snapshot",result="success"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="status_update",protocol="controller-v1",resource="snapshot",result="error"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="status_update",protocol="controller-v1",resource="snapshot",result="forbidden"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="status_update",protocol="controller-v1",resource="snapshot",result="timeout"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="status_update",protocol="controller-v1",resource="snapshot",result="conflict"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="status_update",protocol="controller-v1",resource="snapshot",result="throttled"} 1`,
		`nvidia_dra_cdc_api_actions_total{operation="delete",protocol="controller-v1",resource="snapshot",result="not_found"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="create",protocol="controller-v1",resource="reservation"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="create",protocol="controller-v1",resource="snapshot"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="finalizer_add",protocol="controller-v1",resource="snapshot"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="finalizer_remove",protocol="controller-v1",resource="snapshot"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="status_update",protocol="controller-v1",resource="snapshot"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="delete",protocol="controller-v1",resource="snapshot"} 1`,
		`nvidia_dra_cdc_api_writes_total{operation="attestation_update",protocol="controller-v1",resource="node"} 1`,
	} {
		require.Contains(t, body, expected)
	}
	require.NotContains(t, body, `nvidia_dra_cdc_api_writes_total{operation="get"`)
	require.NotContains(t, body, `nvidia_dra_cdc_api_writes_total{operation="write_barrier_get"`)
}

func TestObserveCliqueWorkloadTimelineUsesDocumentedT0T3Stages(t *testing.T) {
	resetCliqueMetricsForTest()
	t.Cleanup(resetCliqueMetricsForTest)

	t0 := time.Unix(1_000, 0)
	err := ObserveCliqueWorkloadTimeline("controller-v1", CliqueWorkloadTimeline{
		T0WorkloadAccepted:  t0,
		T1WorkloadScheduled: t0.Add(time.Second),
		T2PrepareComplete:   t0.Add(3 * time.Second),
		T3WorkloadReady:     t0.Add(6 * time.Second),
	})
	require.NoError(t, err)

	body := scrapeCliqueMetrics(t)
	for stage, sum := range map[string]string{
		CliqueStageT1MinusT0: "1",
		CliqueStageT2MinusT1: "2",
		CliqueStageT3MinusT2: "3",
		CliqueStageT3MinusT0: "6",
		CliqueStageT3MinusT1: "5",
	} {
		count := `nvidia_dra_cdc_workload_stage_duration_seconds_count{protocol="controller-v1",stage="` + stage + `"} 1`
		require.Contains(t, body, count)
		sumLine := `nvidia_dra_cdc_workload_stage_duration_seconds_sum{protocol="controller-v1",stage="` + stage + `"} ` + sum
		require.True(t, strings.Contains(body, sumLine), "metrics output missing %q", sumLine)
	}

	require.Error(t, ObserveCliqueWorkloadTimeline("controller-v1", CliqueWorkloadTimeline{}))
	require.Error(t, ObserveCliqueWorkloadTimeline("controller-v1", CliqueWorkloadTimeline{
		T0WorkloadAccepted:  t0,
		T1WorkloadScheduled: t0.Add(-time.Second),
		T2PrepareComplete:   t0,
		T3WorkloadReady:     t0,
	}))
}
