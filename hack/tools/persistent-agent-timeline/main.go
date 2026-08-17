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

// persistent-agent-timeline turns captured Kubernetes objects and timestamped
// compute-domain kubelet-plugin logs into the canonical T0-T3 evidence table.
// It intentionally reads files instead of a live cluster: collection and
// analysis stay separate, and the raw evidence remains independently auditable.
package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

type options struct {
	podsPath                 string
	claimsPath               string
	kubeletLogPath           string
	auditPath                string
	outputDir                string
	trialID                  string
	provider                 string
	shape                    string
	expectedPods             int
	allowCreationTimestampT0 bool
	resultPaths              string
	bootstrapSamples         int
	bootstrapSeed            int64
}

type objectMeta struct {
	Name              string    `json:"name"`
	Namespace         string    `json:"namespace"`
	UID               string    `json:"uid"`
	CreationTimestamp time.Time `json:"creationTimestamp"`
}

type podList struct {
	Items []pod `json:"items"`
}

type pod struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		Conditions []condition `json:"conditions"`
	} `json:"status"`
}

type condition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

type claimList struct {
	Items []claim `json:"items"`
}

type claim struct {
	Metadata objectMeta `json:"metadata"`
	Status   struct {
		ReservedFor []struct {
			UID string `json:"uid"`
		} `json:"reservedFor"`
	} `json:"status"`
}

type auditEvent struct {
	Verb           string    `json:"verb"`
	Stage          string    `json:"stage"`
	StageTimestamp time.Time `json:"stageTimestamp"`
	ObjectRef      struct {
		Resource  string `json:"resource"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	} `json:"objectRef"`
	ResponseStatus struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
}

type prepareRecord struct {
	Timestamp time.Time
	Namespace string
	Name      string
	UID       string
}

type timeline struct {
	TrialID       string    `json:"trialID"`
	Provider      string    `json:"provider"`
	Shape         string    `json:"shape"`
	Namespace     string    `json:"namespace"`
	PodName       string    `json:"podName"`
	PodUID        string    `json:"podUID"`
	NodeName      string    `json:"nodeName"`
	ClaimName     string    `json:"claimName"`
	ClaimUID      string    `json:"claimUID"`
	T0Source      string    `json:"t0Source"`
	T0            time.Time `json:"t0"`
	T1            time.Time `json:"t1"`
	T2            time.Time `json:"t2"`
	T3            time.Time `json:"t3"`
	SchedulerMS   float64   `json:"schedulerMS"`
	NodePrepareMS float64   `json:"nodePrepareMS"`
	ReadinessMS   float64   `json:"readinessMS"`
	TotalMS       float64   `json:"totalMS"`
}

type summary struct {
	TrialID       string     `json:"trialID"`
	Provider      string     `json:"provider"`
	Shape         string     `json:"shape"`
	Pods          int        `json:"pods"`
	T0Source      string     `json:"t0Source"`
	SchedulerMS   statistics `json:"schedulerMS"`
	NodePrepareMS statistics `json:"nodePrepareMS"`
	ReadinessMS   statistics `json:"readinessMS"`
	TotalMS       statistics `json:"totalMS"`
	JobMaximumMS  float64    `json:"jobMaximumMS"`
	JobMaximum    statistics `json:"jobMaximumDistributionMS"`
	BootstrapSeed int64      `json:"bootstrapSeed"`
	BootstrapRuns int        `json:"bootstrapRuns"`
	Timelines     []timeline `json:"timelines,omitempty"`
}

type statistics struct {
	Minimum         float64            `json:"minimum"`
	P50             float64            `json:"p50"`
	P95             float64            `json:"p95"`
	P99             float64            `json:"p99"`
	Maximum         float64            `json:"maximum"`
	P50Confidence95 confidenceInterval `json:"p50Confidence95"`
	P95Confidence95 confidenceInterval `json:"p95Confidence95"`
}

type confidenceInterval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

var preparedClaimRE = regexp.MustCompile(`Prepared devices for claim '([^/[:space:]]+)/([^:[:space:]]+):([^'[:space:]]+)'`)

func main() {
	opts := parseFlags()
	if err := run(opts); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.podsPath, "pods", "", "captured PodList JSON")
	flag.StringVar(&opts.claimsPath, "claims", "", "captured ResourceClaimList JSON")
	flag.StringVar(&opts.kubeletLogPath, "kubelet-log", "", "timestamped compute-domain kubelet-plugin log")
	flag.StringVar(&opts.auditPath, "audit-log", "", "optional apiserver audit JSONL")
	flag.StringVar(&opts.outputDir, "output-dir", "", "output directory")
	flag.StringVar(&opts.trialID, "trial-id", "", "stable trial identity")
	flag.StringVar(&opts.provider, "provider", "", "legacy-v1 or persistent-agent-v1")
	flag.StringVar(&opts.shape, "shape", "", "shape label, for example 2x1 or 1x18")
	flag.IntVar(&opts.expectedPods, "expected-pods", 0, "exact expected workload Pod count")
	flag.BoolVar(&opts.allowCreationTimestampT0, "allow-creation-timestamp-t0", false, "allow Pod creationTimestamp when audit ResponseComplete is unavailable")
	flag.StringVar(&opts.resultPaths, "results", "", "comma-separated result.json files to aggregate")
	flag.IntVar(&opts.bootstrapSamples, "bootstrap-samples", 2000, "deterministic trial-cluster bootstrap repetitions")
	flag.Int64Var(&opts.bootstrapSeed, "bootstrap-seed", 20260817, "deterministic bootstrap seed")
	flag.Parse()
	return opts
}

func run(opts options) error {
	if opts.outputDir == "" {
		return errors.New("--output-dir is required")
	}
	if opts.bootstrapSamples < 0 {
		return errors.New("--bootstrap-samples must not be negative")
	}
	if err := os.MkdirAll(opts.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if opts.resultPaths != "" {
		return aggregateResults(opts)
	}
	for name, value := range map[string]string{
		"--pods": opts.podsPath, "--claims": opts.claimsPath,
		"--kubelet-log": opts.kubeletLogPath, "--trial-id": opts.trialID,
		"--provider": opts.provider, "--shape": opts.shape,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if opts.expectedPods < 1 {
		return errors.New("--expected-pods must be positive")
	}

	pods, err := readJSON[podList](opts.podsPath)
	if err != nil {
		return err
	}
	claims, err := readJSON[claimList](opts.claimsPath)
	if err != nil {
		return err
	}
	prepares, err := readPrepareLog(opts.kubeletLogPath)
	if err != nil {
		return err
	}
	auditTimes := map[string]time.Time{}
	if opts.auditPath != "" {
		auditTimes, err = readAuditLog(opts.auditPath)
		if err != nil {
			return err
		}
	}

	timelines, err := buildTimelines(opts, pods.Items, claims.Items, prepares, auditTimes)
	if err != nil {
		return err
	}
	result := summarize(opts.trialID, opts.provider, opts.shape, timelines, opts.bootstrapSeed, opts.bootstrapSamples)
	if err := writeTimelineCSV(filepath.Join(opts.outputDir, "timeline.csv"), timelines); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(opts.outputDir, "result.json"), result); err != nil {
		return err
	}
	return nil
}

func buildTimelines(opts options, pods []pod, claims []claim, prepares []prepareRecord, auditTimes map[string]time.Time) ([]timeline, error) {
	if len(pods) != opts.expectedPods {
		return nil, fmt.Errorf("observed %d workload Pods, want exactly %d", len(pods), opts.expectedPods)
	}
	claimByPod := map[string][]claim{}
	for _, item := range claims {
		for _, ref := range item.Status.ReservedFor {
			claimByPod[ref.UID] = append(claimByPod[ref.UID], item)
		}
	}
	prepareByUID := map[string]prepareRecord{}
	for _, item := range prepares {
		if previous, found := prepareByUID[item.UID]; !found || item.Timestamp.Before(previous.Timestamp) {
			prepareByUID[item.UID] = item
		}
	}

	result := make([]timeline, 0, len(pods))
	for _, item := range pods {
		t0, source := auditTimes[auditKey(item.Metadata.Namespace, item.Metadata.Name)], "audit-response-complete"
		if t0.IsZero() {
			if !opts.allowCreationTimestampT0 {
				return nil, fmt.Errorf("Pod %s/%s has no audit ResponseComplete T0; supply --audit-log or explicitly allow creationTimestamp fallback", item.Metadata.Namespace, item.Metadata.Name)
			}
			t0, source = item.Metadata.CreationTimestamp, "pod-creation-timestamp"
		}
		t1 := conditionTime(item.Status.Conditions, "PodScheduled")
		t3 := conditionTime(item.Status.Conditions, "Ready")
		if t0.IsZero() || t1.IsZero() || t3.IsZero() {
			return nil, fmt.Errorf("Pod %s/%s is missing T0, PodScheduled=True, or Ready=True", item.Metadata.Namespace, item.Metadata.Name)
		}

		var selectedClaim claim
		var selectedPrepare prepareRecord
		for _, candidate := range claimByPod[item.Metadata.UID] {
			if prepared, found := prepareByUID[candidate.Metadata.UID]; found {
				if !selectedPrepare.Timestamp.IsZero() {
					return nil, fmt.Errorf("Pod %s/%s has more than one successfully prepared reserved claim; the Tier C fixture must have exactly one measured ComputeDomain channel claim", item.Metadata.Namespace, item.Metadata.Name)
				}
				selectedClaim, selectedPrepare = candidate, prepared
			}
		}
		if selectedPrepare.Timestamp.IsZero() {
			return nil, fmt.Errorf("Pod %s/%s has no exact successful compute-domain NodePrepare log for any reserved claim", item.Metadata.Namespace, item.Metadata.Name)
		}
		if selectedPrepare.Namespace != selectedClaim.Metadata.Namespace || selectedPrepare.Name != selectedClaim.Metadata.Name {
			return nil, fmt.Errorf("claim identity mismatch for UID %s", selectedClaim.Metadata.UID)
		}
		if t1.After(selectedPrepare.Timestamp) || selectedPrepare.Timestamp.After(t3) || t0.After(t1) {
			return nil, fmt.Errorf("invalid T0-T3 ordering for Pod %s/%s: %s %s %s %s", item.Metadata.Namespace, item.Metadata.Name, t0, t1, selectedPrepare.Timestamp, t3)
		}

		result = append(result, timeline{
			TrialID: opts.trialID, Provider: opts.provider, Shape: opts.shape,
			Namespace: item.Metadata.Namespace, PodName: item.Metadata.Name,
			PodUID: item.Metadata.UID, NodeName: item.Spec.NodeName,
			ClaimName: selectedClaim.Metadata.Name, ClaimUID: selectedClaim.Metadata.UID,
			T0Source: source, T0: t0, T1: t1, T2: selectedPrepare.Timestamp, T3: t3,
			SchedulerMS: durationMS(t0, t1), NodePrepareMS: durationMS(t1, selectedPrepare.Timestamp),
			ReadinessMS: durationMS(selectedPrepare.Timestamp, t3), TotalMS: durationMS(t0, t3),
		})
	}
	slices.SortFunc(result, func(a, b timeline) int {
		return strings.Compare(a.Namespace+"/"+a.PodName, b.Namespace+"/"+b.PodName)
	})
	return result, nil
}

func conditionTime(conditions []condition, conditionType string) time.Time {
	for _, item := range conditions {
		if item.Type == conditionType && item.Status == "True" {
			return item.LastTransitionTime
		}
	}
	return time.Time{}
}

func readPrepareLog(path string) ([]prepareRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open kubelet log: %w", err)
	}
	defer file.Close()

	var result []prepareRecord
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		space := strings.IndexByte(line, ' ')
		if space <= 0 {
			continue
		}
		timestamp, err := time.Parse(time.RFC3339Nano, line[:space])
		if err != nil {
			continue
		}
		match := preparedClaimRE.FindStringSubmatch(line[space+1:])
		if len(match) != 4 {
			continue
		}
		result = append(result, prepareRecord{Timestamp: timestamp, Namespace: match[1], Name: match[2], UID: match[3]})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan kubelet log: %w", err)
	}
	return result, nil
}

func readAuditLog(path string) (map[string]time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	defer file.Close()

	result := map[string]time.Time{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var event auditEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode audit event: %w", err)
		}
		if event.Verb != "create" || event.Stage != "ResponseComplete" || event.ObjectRef.Resource != "pods" || event.ResponseStatus.Code < 200 || event.ResponseStatus.Code >= 300 {
			continue
		}
		key := auditKey(event.ObjectRef.Namespace, event.ObjectRef.Name)
		if previous, found := result[key]; !found || event.StageTimestamp.Before(previous) {
			result[key] = event.StageTimestamp
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan audit log: %w", err)
	}
	return result, nil
}

func auditKey(namespace, name string) string { return namespace + "/" + name }

func summarize(trialID, provider, shape string, timelines []timeline, seed int64, bootstrapRuns int) summary {
	result := summary{
		TrialID: trialID, Provider: provider, Shape: shape, Pods: len(timelines), Timelines: timelines,
		BootstrapSeed: seed, BootstrapRuns: bootstrapRuns,
	}
	if len(timelines) == 0 {
		return result
	}
	result.T0Source = timelines[0].T0Source
	jobMaximumByTrial := map[string]float64{}
	for _, item := range timelines {
		if item.TotalMS > jobMaximumByTrial[item.TrialID] {
			jobMaximumByTrial[item.TrialID] = item.TotalMS
		}
		if item.TotalMS > result.JobMaximumMS {
			result.JobMaximumMS = item.TotalMS
		}
		if item.T0Source != result.T0Source {
			result.T0Source = "mixed"
		}
	}
	result.SchedulerMS = timelineStats(timelines, func(item timeline) float64 { return item.SchedulerMS }, seed+1, bootstrapRuns)
	result.NodePrepareMS = timelineStats(timelines, func(item timeline) float64 { return item.NodePrepareMS }, seed+2, bootstrapRuns)
	result.ReadinessMS = timelineStats(timelines, func(item timeline) float64 { return item.ReadinessMS }, seed+3, bootstrapRuns)
	result.TotalMS = timelineStats(timelines, func(item timeline) float64 { return item.TotalMS }, seed+4, bootstrapRuns)
	trialIDs := make([]string, 0, len(jobMaximumByTrial))
	for trialID := range jobMaximumByTrial {
		trialIDs = append(trialIDs, trialID)
	}
	slices.Sort(trialIDs)
	jobMaximums := make([]float64, 0, len(trialIDs))
	for _, trialID := range trialIDs {
		jobMaximums = append(jobMaximums, jobMaximumByTrial[trialID])
	}
	result.JobMaximum = valueStats(jobMaximums, seed+5, bootstrapRuns)
	return result
}

func timelineStats(timelines []timeline, value func(timeline) float64, seed int64, bootstrapRuns int) statistics {
	values := make([]float64, 0, len(timelines))
	byTrial := map[string][]float64{}
	for _, item := range timelines {
		measured := value(item)
		values = append(values, measured)
		byTrial[item.TrialID] = append(byTrial[item.TrialID], measured)
	}
	result := baseStats(values)
	if bootstrapRuns == 0 || len(byTrial) == 0 {
		return result
	}
	trialIDs := make([]string, 0, len(byTrial))
	for trialID := range byTrial {
		trialIDs = append(trialIDs, trialID)
	}
	slices.Sort(trialIDs)
	trials := make([][]float64, 0, len(trialIDs))
	for _, trialID := range trialIDs {
		trials = append(trials, byTrial[trialID])
	}
	random := rand.New(rand.NewSource(seed)) // #nosec G404 -- reproducible statistical resampling, not cryptography.
	p50s, p95s := make([]float64, 0, bootstrapRuns), make([]float64, 0, bootstrapRuns)
	for range bootstrapRuns {
		var sample []float64
		for range trials {
			sample = append(sample, trials[random.Intn(len(trials))]...)
		}
		slices.Sort(sample)
		p50s = append(p50s, nearestRank(sample, 0.50))
		p95s = append(p95s, nearestRank(sample, 0.95))
	}
	result.P50Confidence95 = confidence95(p50s)
	result.P95Confidence95 = confidence95(p95s)
	return result
}

func valueStats(values []float64, seed int64, bootstrapRuns int) statistics {
	result := baseStats(values)
	if bootstrapRuns == 0 || len(values) == 0 {
		return result
	}
	random := rand.New(rand.NewSource(seed)) // #nosec G404 -- reproducible statistical resampling, not cryptography.
	p50s, p95s := make([]float64, 0, bootstrapRuns), make([]float64, 0, bootstrapRuns)
	sample := make([]float64, len(values))
	for range bootstrapRuns {
		for index := range sample {
			sample[index] = values[random.Intn(len(values))]
		}
		slices.Sort(sample)
		p50s = append(p50s, nearestRank(sample, 0.50))
		p95s = append(p95s, nearestRank(sample, 0.95))
	}
	result.P50Confidence95 = confidence95(p50s)
	result.P95Confidence95 = confidence95(p95s)
	return result
}

func baseStats(values []float64) statistics {
	if len(values) == 0 {
		return statistics{}
	}
	values = slices.Clone(values)
	slices.Sort(values)
	return statistics{
		Minimum: values[0], P50: nearestRank(values, 0.50),
		P95: nearestRank(values, 0.95), P99: nearestRank(values, 0.99),
		Maximum: values[len(values)-1],
	}
}

func confidence95(values []float64) confidenceInterval {
	slices.Sort(values)
	return confidenceInterval{Lower: nearestRank(values, 0.025), Upper: nearestRank(values, 0.975)}
}

func nearestRank(values []float64, percentile float64) float64 {
	index := int(float64(len(values))*percentile+0.999999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func durationMS(start, end time.Time) float64 {
	return float64(end.Sub(start).Microseconds()) / 1000
}

func writeTimelineCSV(path string, timelines []timeline) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create timeline CSV: %w", err)
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err := w.Write([]string{"trial_id", "provider", "shape", "namespace", "pod", "pod_uid", "node", "claim", "claim_uid", "t0_source", "t0", "t1", "t2", "t3", "scheduler_ms", "nodeprepare_ms", "readiness_ms", "total_ms"}); err != nil {
		return err
	}
	for _, item := range timelines {
		record := []string{
			item.TrialID, item.Provider, item.Shape, item.Namespace, item.PodName,
			item.PodUID, item.NodeName, item.ClaimName, item.ClaimUID, item.T0Source,
			item.T0.Format(time.RFC3339Nano), item.T1.Format(time.RFC3339Nano),
			item.T2.Format(time.RFC3339Nano), item.T3.Format(time.RFC3339Nano),
			formatFloat(item.SchedulerMS), formatFloat(item.NodePrepareMS),
			formatFloat(item.ReadinessMS), formatFloat(item.TotalMS),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func aggregateResults(opts options) error {
	paths := strings.Split(opts.resultPaths, ",")
	var timelines []timeline
	for _, path := range paths {
		item, err := readJSON[summary](strings.TrimSpace(path))
		if err != nil {
			return err
		}
		timelines = append(timelines, item.Timelines...)
	}
	if len(timelines) == 0 {
		return errors.New("no timelines found in --results")
	}
	provider, shape := timelines[0].Provider, timelines[0].Shape
	for _, item := range timelines {
		if item.Provider != provider || item.Shape != shape {
			return errors.New("all aggregate results must have the same provider and shape")
		}
	}
	result := summarize("aggregate", provider, shape, timelines, opts.bootstrapSeed, opts.bootstrapSamples)
	if err := writeTimelineCSV(filepath.Join(opts.outputDir, "timeline.csv"), timelines); err != nil {
		return err
	}
	return writeJSON(filepath.Join(opts.outputDir, "result.json"), result)
}

func readJSON[T any](path string) (T, error) {
	var result T
	data, err := os.ReadFile(path)
	if err != nil {
		return result, fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("decode %s: %w", path, err)
	}
	return result, nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 3, 64)
}
