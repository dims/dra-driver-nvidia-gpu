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

package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCollectExactTimeline(t *testing.T) {
	dir := t.TempDir()
	podsPath := filepath.Join(dir, "pods.json")
	claimsPath := filepath.Join(dir, "claims.json")
	logPath := filepath.Join(dir, "kubelet.log")
	auditPath := filepath.Join(dir, "audit.jsonl")
	outputDir := filepath.Join(dir, "result")

	writeTestFile(t, podsPath, `{
  "items": [
    {
      "metadata": {"namespace":"trial","name":"worker-0","uid":"pod-0","creationTimestamp":"2026-08-17T12:00:00.100Z"},
      "spec": {"nodeName":"node-0"},
      "status": {"conditions":[
        {"type":"PodScheduled","status":"True","lastTransitionTime":"2026-08-17T12:00:01Z"},
        {"type":"Ready","status":"True","lastTransitionTime":"2026-08-17T12:00:05Z"}
      ]}
    },
    {
      "metadata": {"namespace":"trial","name":"worker-1","uid":"pod-1","creationTimestamp":"2026-08-17T12:00:00.200Z"},
      "spec": {"nodeName":"node-1"},
      "status": {"conditions":[
        {"type":"PodScheduled","status":"True","lastTransitionTime":"2026-08-17T12:00:02Z"},
        {"type":"Ready","status":"True","lastTransitionTime":"2026-08-17T12:00:07Z"}
      ]}
    }
  ]
}`)
	writeTestFile(t, claimsPath, `{
  "items": [
    {"metadata":{"namespace":"trial","name":"channel-0","uid":"claim-0"},"status":{"reservedFor":[{"uid":"pod-0"}]}},
    {"metadata":{"namespace":"trial","name":"channel-1","uid":"claim-1"},"status":{"reservedFor":[{"uid":"pod-1"}]}}
  ]
}`)
	writeTestFile(t, logPath, "2026-08-17T12:00:03.000000000Z I0817 Prepared devices for claim 'trial/channel-0:claim-0': []\n"+
		"2026-08-17T12:00:04.000000000Z I0817 Prepared devices for claim 'trial/channel-1:claim-1': []\n")
	writeTestFile(t, auditPath,
		`{"verb":"create","stage":"ResponseComplete","stageTimestamp":"2026-08-17T12:00:00Z","objectRef":{"resource":"pods","namespace":"trial","name":"worker-0"},"responseStatus":{"code":201}}`+"\n"+
			`{"verb":"create","stage":"ResponseComplete","stageTimestamp":"2026-08-17T12:00:00.050Z","objectRef":{"resource":"pods","namespace":"trial","name":"worker-1"},"responseStatus":{"code":201}}`+"\n")

	opts := options{
		podsPath: podsPath, claimsPath: claimsPath, kubeletLogPath: logPath,
		auditPath: auditPath, outputDir: outputDir, trialID: "trial-1",
		provider: "persistent-agent-v1", shape: "2x1", expectedPods: 2,
	}
	if err := run(opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	result, err := readJSON[summary](filepath.Join(outputDir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Pods != 2 || result.T0Source != "audit-response-complete" {
		t.Fatalf("unexpected summary: %+v", result)
	}
	if result.TotalMS.P50 != 5000 || result.TotalMS.P95 != 6950 || result.JobMaximumMS != 6950 {
		t.Fatalf("unexpected totals: %+v", result.TotalMS)
	}
	if result.Timelines[0].ClaimUID != "claim-0" || result.Timelines[1].NodePrepareMS != 2000 {
		t.Fatalf("unexpected timelines: %+v", result.Timelines)
	}
}

func TestCreationTimestampRequiresExplicitFallback(t *testing.T) {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	pods := []pod{testPod(base)}
	claims := []claim{testClaim()}
	prepares := []prepareRecord{{Timestamp: base.Add(2 * time.Second), Namespace: "trial", Name: "channel", UID: "claim"}}
	opts := options{trialID: "trial", provider: "legacy-v1", shape: "1x1", expectedPods: 1}
	if _, err := buildTimelines(opts, pods, claims, prepares, nil); err == nil {
		t.Fatal("expected missing audit T0 to fail")
	}
	opts.allowCreationTimestampT0 = true
	timelines, err := buildTimelines(opts, pods, claims, prepares, nil)
	if err != nil {
		t.Fatal(err)
	}
	if timelines[0].T0Source != "pod-creation-timestamp" {
		t.Fatalf("unexpected T0 source: %s", timelines[0].T0Source)
	}
}

func TestInvalidOrderingFails(t *testing.T) {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	pods := []pod{testPod(base)}
	claims := []claim{testClaim()}
	prepares := []prepareRecord{{Timestamp: base.Add(500 * time.Millisecond), Namespace: "trial", Name: "channel", UID: "claim"}}
	opts := options{trialID: "trial", provider: "legacy-v1", shape: "1x1", expectedPods: 1, allowCreationTimestampT0: true}
	if _, err := buildTimelines(opts, pods, claims, prepares, nil); err == nil {
		t.Fatal("expected T2 before T1 to fail")
	}
}

func TestPrepareInReadyTimestampSecondIsAccepted(t *testing.T) {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	pods := []pod{testPod(base)}
	claims := []claim{testClaim()}
	prepares := []prepareRecord{{Timestamp: base.Add(3500 * time.Millisecond), Namespace: "trial", Name: "channel", UID: "claim"}}
	opts := options{trialID: "trial", provider: "legacy-v1", shape: "1x1", expectedPods: 1, allowCreationTimestampT0: true}

	timelines, err := buildTimelines(opts, pods, claims, prepares, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := timelines[0]; !got.T3.Equal(got.T2) || got.ReadinessMS != 0 || got.TotalMS != 3500 {
		t.Fatalf("same-second T3 was not normalized: %+v", got)
	}
}

func TestPrepareAfterReadyTimestampSecondFails(t *testing.T) {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	pods := []pod{testPod(base)}
	claims := []claim{testClaim()}
	prepares := []prepareRecord{{Timestamp: base.Add(4 * time.Second), Namespace: "trial", Name: "channel", UID: "claim"}}
	opts := options{trialID: "trial", provider: "legacy-v1", shape: "1x1", expectedPods: 1, allowCreationTimestampT0: true}

	if _, err := buildTimelines(opts, pods, claims, prepares, nil); err == nil {
		t.Fatal("expected T2 in a later second than T3 to fail")
	}
}

func TestFirstSuccessfulPrepareIsT2(t *testing.T) {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	pods := []pod{testPod(base)}
	claims := []claim{testClaim()}
	prepares := []prepareRecord{
		{Timestamp: base.Add(2500 * time.Millisecond), Namespace: "trial", Name: "channel", UID: "claim"},
		{Timestamp: base.Add(2 * time.Second), Namespace: "trial", Name: "channel", UID: "claim"},
	}
	opts := options{trialID: "trial", provider: "legacy-v1", shape: "1x1", expectedPods: 1, allowCreationTimestampT0: true}
	timelines, err := buildTimelines(opts, pods, claims, prepares, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := timelines[0].T2, base.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("T2 = %v, want first success %v", got, want)
	}
}

func TestMultiplePreparedClaimsFail(t *testing.T) {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	pods := []pod{testPod(base)}
	claims := []claim{testClaim(), testClaim()}
	claims[1].Metadata.Name = "other"
	claims[1].Metadata.UID = "other-claim"
	prepares := []prepareRecord{
		{Timestamp: base.Add(2 * time.Second), Namespace: "trial", Name: "channel", UID: "claim"},
		{Timestamp: base.Add(2500 * time.Millisecond), Namespace: "trial", Name: "other", UID: "other-claim"},
	}
	opts := options{trialID: "trial", provider: "legacy-v1", shape: "1x1", expectedPods: 1, allowCreationTimestampT0: true}
	if _, err := buildTimelines(opts, pods, claims, prepares, nil); err == nil {
		t.Fatal("expected multiple prepared claims to fail")
	}
}

func TestTrialClusterBootstrapIsDeterministic(t *testing.T) {
	timelines := []timeline{
		{TrialID: "a", SchedulerMS: 1, NodePrepareMS: 2, ReadinessMS: 3, TotalMS: 6},
		{TrialID: "a", SchedulerMS: 2, NodePrepareMS: 3, ReadinessMS: 4, TotalMS: 9},
		{TrialID: "b", SchedulerMS: 10, NodePrepareMS: 20, ReadinessMS: 30, TotalMS: 60},
		{TrialID: "b", SchedulerMS: 20, NodePrepareMS: 30, ReadinessMS: 40, TotalMS: 90},
	}
	first := summarize("aggregate", "persistent-agent-v1", "1x2", timelines, 42, 200)
	second := summarize("aggregate", "persistent-agent-v1", "1x2", slicesCloneReversed(timelines), 42, 200)
	if !reflect.DeepEqual(first.SchedulerMS, second.SchedulerMS) || !reflect.DeepEqual(first.JobMaximum, second.JobMaximum) {
		t.Fatalf("bootstrap changed with input order: first=%+v/%+v second=%+v/%+v", first.SchedulerMS, first.JobMaximum, second.SchedulerMS, second.JobMaximum)
	}
	if first.SchedulerMS.P95Confidence95 == (confidenceInterval{}) || first.JobMaximum.P95Confidence95 == (confidenceInterval{}) {
		t.Fatalf("missing confidence interval: %+v", first)
	}
}

func slicesCloneReversed(values []timeline) []timeline {
	result := append([]timeline(nil), values...)
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}

func testPod(base time.Time) pod {
	var result pod
	result.Metadata = objectMeta{Name: "worker", Namespace: "trial", UID: "pod", CreationTimestamp: base}
	result.Spec.NodeName = "node"
	result.Status.Conditions = []condition{
		{Type: "PodScheduled", Status: "True", LastTransitionTime: base.Add(time.Second)},
		{Type: "Ready", Status: "True", LastTransitionTime: base.Add(3 * time.Second)},
	}
	return result
}

func testClaim() claim {
	var result claim
	result.Metadata = objectMeta{Name: "channel", Namespace: "trial", UID: "claim"}
	result.Status.ReservedFor = append(result.Status.ReservedFor, struct {
		UID string `json:"uid"`
	}{UID: "pod"})
	return result
}

func writeTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
		t.Fatal(err)
	}
}
