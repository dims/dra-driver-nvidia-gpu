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

// persistent-agent-comparison compares paired, independently installed main
// and latest-branch Tier C sessions. It consumes preserved result files; it
// never reaches into a live cluster or invents missing evidence.
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
	"sort"
	"strconv"
	"strings"
)

type options struct {
	manifestPath     string
	installationPath string
	outputDir        string
	expectedBlocks   int
	expectedTrials   int
	bootstrapSamples int
	bootstrapSeed    int64
	enforce          bool
}

type manifestRow struct {
	Block         int
	Arm           string
	Scenario      string
	ResultPath    string
	LifecyclePath string
	ArtifactDir   string
}

type timelineResult struct {
	Timelines []timeline `json:"timelines"`
}

type timeline struct {
	TrialID       string  `json:"trialID"`
	SchedulerMS   float64 `json:"schedulerMS"`
	NodePrepareMS float64 `json:"nodePrepareMS"`
	ReadinessMS   float64 `json:"readinessMS"`
	TotalMS       float64 `json:"totalMS"`
}

type lifecycle struct {
	MeasurementVersion string  `json:"measurementVersion"`
	TrialID            string  `json:"trialID"`
	CycleClass         string  `json:"cycleClass"`
	ReuseReadyMS       float64 `json:"reuseReadyMS"`
	FenceMS            float64 `json:"fenceMS"`
	FinalizationMS     float64 `json:"finalizationMS"`
}

const watchReceiptMeasurementVersion = "watch-receipt-v1"

type statistics struct {
	Count        int     `json:"count"`
	Minimum      float64 `json:"minimum"`
	P50          float64 `json:"p50"`
	P90          float64 `json:"p90"`
	P95          float64 `json:"p95"`
	Maximum      float64 `json:"maximum"`
	FirstQuarter float64 `json:"firstQuarterP50"`
	LastQuarter  float64 `json:"lastQuarterP50"`
	DriftRatio   float64 `json:"driftRatio"`
}

type churn struct {
	Bytes  int64          `json:"bytes"`
	Events int            `json:"events"`
	ByKind map[string]int `json:"eventsByKind"`
}

type armScenario struct {
	Arm              string                `json:"arm"`
	Scenario         string                `json:"scenario"`
	Trials           int                   `json:"trials"`
	Stages           map[string]statistics `json:"stages"`
	Lifecycle        map[string]statistics `json:"lifecycle,omitempty"`
	Churn            churn                 `json:"churn"`
	WorkloadImageIDs []string              `json:"workloadImageIDs"`
	Nodes            []string              `json:"nodes"`
}

type interval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

type comparison struct {
	Scenario      string   `json:"scenario"`
	Stage         string   `json:"stage"`
	MainP50       float64  `json:"mainP50"`
	BranchP50     float64  `json:"branchP50"`
	P50Ratio      float64  `json:"p50Ratio"`
	MainP95       float64  `json:"mainP95"`
	BranchP95     float64  `json:"branchP95"`
	P95Ratio      float64  `json:"p95Ratio"`
	MedianDelta95 interval `json:"pairedBlockMedianDelta95"`
}

type report struct {
	ExpectedBlocks           int                   `json:"expectedBlocks"`
	ExpectedTrials           int                   `json:"expectedTrialsPerBlock"`
	BootstrapSeed            int64                 `json:"bootstrapSeed"`
	BootstrapRuns            int                   `json:"bootstrapRuns"`
	Sessions                 []armScenario         `json:"sessions"`
	Comparisons              []comparison          `json:"comparisons"`
	Checks                   []check               `json:"checks"`
	Passed                   bool                  `json:"passed"`
	Installations            map[string]statistics `json:"installations"`
	IncrementalFleetWarmupMS float64               `json:"incrementalFleetWarmupMS"`
	LatencyBreakEvenDomains  float64               `json:"latencyBreakEvenDomains"`
}

type check struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details"`
}

var watchEventRE = regexp.MustCompile(`"type"\s*:\s*"(ADDED|MODIFIED|DELETED)"`)

func main() {
	var opts options
	flag.StringVar(&opts.manifestPath, "manifest", "", "CSV manifest produced by the two-node performance runner")
	flag.StringVar(&opts.installationPath, "installations", "", "CSV installation timings produced by the two-node performance runner")
	flag.StringVar(&opts.outputDir, "output-dir", "", "report output directory")
	flag.IntVar(&opts.expectedBlocks, "expected-blocks", 4, "exact expected paired block count")
	flag.IntVar(&opts.expectedTrials, "expected-trials", 25, "exact measured trials per block and scenario")
	flag.IntVar(&opts.bootstrapSamples, "bootstrap-samples", 10000, "paired block bootstrap repetitions")
	flag.Int64Var(&opts.bootstrapSeed, "bootstrap-seed", 20260818, "deterministic bootstrap seed")
	flag.BoolVar(&opts.enforce, "enforce", false, "exit nonzero when acceptance checks fail")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	if opts.manifestPath == "" || opts.installationPath == "" || opts.outputDir == "" {
		return errors.New("--manifest, --installations, and --output-dir are required")
	}
	if opts.expectedBlocks < 1 || opts.expectedTrials < 1 || opts.bootstrapSamples < 1 {
		return errors.New("expected blocks, trials, and bootstrap samples must be positive")
	}
	rows, err := readManifest(opts.manifestPath)
	if err != nil {
		return err
	}
	report, err := buildReport(opts, rows)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(opts.outputDir, 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(opts.outputDir, "comparison.json"), report); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(opts.outputDir, "comparison.csv"), report); err != nil {
		return err
	}
	if err := writeMarkdown(filepath.Join(opts.outputDir, "report.md"), report); err != nil {
		return err
	}
	if opts.enforce && !report.Passed {
		return errors.New("one or more two-node performance acceptance checks failed; see report.md")
	}
	return nil
}

func readManifest(path string) ([]manifestRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 || !slices.Equal(records[0], []string{"block", "arm", "scenario", "result", "lifecycle", "artifacts"}) {
		return nil, errors.New("manifest header must be block,arm,scenario,result,lifecycle,artifacts")
	}
	rows := make([]manifestRow, 0, len(records)-1)
	for index, record := range records[1:] {
		if len(record) != 6 {
			return nil, fmt.Errorf("manifest row %d has %d fields, want 6", index+2, len(record))
		}
		block, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, fmt.Errorf("manifest row %d block: %w", index+2, err)
		}
		if record[1] != "M" && record[1] != "B" {
			return nil, fmt.Errorf("manifest row %d arm must be M or B", index+2)
		}
		if record[2] != "cold-domain" && record[2] != "warm-workload" {
			return nil, fmt.Errorf("manifest row %d has invalid scenario %q", index+2, record[2])
		}
		rows = append(rows, manifestRow{Block: block, Arm: record[1], Scenario: record[2], ResultPath: record[3], LifecyclePath: record[4], ArtifactDir: record[5]})
	}
	return rows, nil
}

func buildReport(opts options, rows []manifestRow) (report, error) {
	type bucket struct {
		timelines  []timeline
		lifecycles []lifecycle
		trials     map[string]struct{}
		churn      churn
		blocks     map[int][]timeline
		imageIDs   map[string]struct{}
		nodes      map[string]struct{}
	}
	buckets := map[string]*bucket{}
	seenRows := map[string]bool{}
	for _, row := range rows {
		rowKey := fmt.Sprintf("%d/%s/%s", row.Block, row.Arm, row.Scenario)
		if seenRows[rowKey] {
			return report{}, fmt.Errorf("duplicate manifest row %s", rowKey)
		}
		seenRows[rowKey] = true
		var result timelineResult
		if err := readJSON(row.ResultPath, &result); err != nil {
			return report{}, fmt.Errorf("read %s: %w", row.ResultPath, err)
		}
		key := row.Arm + "/" + row.Scenario
		if buckets[key] == nil {
			buckets[key] = &bucket{trials: map[string]struct{}{}, churn: churn{ByKind: map[string]int{}}, blocks: map[int][]timeline{}, imageIDs: map[string]struct{}{}, nodes: map[string]struct{}{}}
		}
		b := buckets[key]
		b.timelines = append(b.timelines, result.Timelines...)
		b.blocks[row.Block] = append(b.blocks[row.Block], result.Timelines...)
		for _, item := range result.Timelines {
			b.trials[item.TrialID] = struct{}{}
		}
		lifecycles, err := readLifecycle(row.LifecyclePath)
		if err != nil {
			return report{}, err
		}
		for _, item := range lifecycles {
			if item.CycleClass == "measured" {
				if err := validateLifecycleMeasurement(item); err != nil {
					return report{}, fmt.Errorf("%s: %w", row.LifecyclePath, err)
				}
				b.lifecycles = append(b.lifecycles, item)
			}
		}
		observedChurn, err := readChurn(row.ArtifactDir)
		if err != nil {
			return report{}, err
		}
		b.churn.Bytes += observedChurn.Bytes
		b.churn.Events += observedChurn.Events
		for kind, count := range observedChurn.ByKind {
			b.churn.ByKind[kind] += count
		}
		imageIDs, nodes, err := readWorkloadIdentities(row.ArtifactDir)
		if err != nil {
			return report{}, err
		}
		for _, value := range imageIDs {
			b.imageIDs[value] = struct{}{}
		}
		for _, value := range nodes {
			b.nodes[value] = struct{}{}
		}
	}

	wantRows := opts.expectedBlocks * 4
	if len(seenRows) != wantRows {
		return report{}, fmt.Errorf("observed %d unique arm/scenario block rows, want %d", len(seenRows), wantRows)
	}
	result := report{ExpectedBlocks: opts.expectedBlocks, ExpectedTrials: opts.expectedTrials, BootstrapSeed: opts.bootstrapSeed, BootstrapRuns: opts.bootstrapSamples, Passed: true}
	for _, arm := range []string{"M", "B"} {
		for _, scenario := range []string{"cold-domain", "warm-workload"} {
			key := arm + "/" + scenario
			b := buckets[key]
			if b == nil {
				return report{}, fmt.Errorf("missing %s", key)
			}
			wantTrials := opts.expectedBlocks * opts.expectedTrials
			if len(b.trials) != wantTrials {
				return report{}, fmt.Errorf("%s has %d unique measured trials, want %d", key, len(b.trials), wantTrials)
			}
			entry := armScenario{Arm: arm, Scenario: scenario, Trials: len(b.trials), Stages: stageStatistics(b.timelines), Churn: b.churn, WorkloadImageIDs: sortedKeys(b.imageIDs), Nodes: sortedKeys(b.nodes)}
			if scenario == "cold-domain" {
				if len(b.lifecycles) != wantTrials {
					return report{}, fmt.Errorf("%s has %d measured D0-D4 records, want %d", key, len(b.lifecycles), wantTrials)
				}
				entry.Lifecycle = lifecycleStatistics(b.lifecycles)
			}
			result.Sessions = append(result.Sessions, entry)
		}
	}

	for _, scenario := range []string{"cold-domain", "warm-workload"} {
		for _, stage := range []string{"scheduler", "nodePrepare", "readiness", "total"} {
			mainStats := findSession(result.Sessions, "M", scenario).Stages[stage]
			branchStats := findSession(result.Sessions, "B", scenario).Stages[stage]
			result.Comparisons = append(result.Comparisons, comparison{
				Scenario: scenario, Stage: stage,
				MainP50: mainStats.P50, BranchP50: branchStats.P50, P50Ratio: ratio(branchStats.P50, mainStats.P50),
				MainP95: mainStats.P95, BranchP95: branchStats.P95, P95Ratio: ratio(branchStats.P95, mainStats.P95),
				MedianDelta95: pairedBlockInterval(buckets["M/"+scenario].blocks, buckets["B/"+scenario].blocks, stage, opts),
			})
		}
	}
	installationValues, err := readInstallations(opts.installationPath, opts.expectedBlocks)
	if err != nil {
		return report{}, err
	}
	result.Installations = map[string]statistics{"M": stats(installationValues["M"]), "B": stats(installationValues["B"])}
	result.IncrementalFleetWarmupMS = result.Installations["B"].P50 - result.Installations["M"].P50
	if result.IncrementalFleetWarmupMS < 0 {
		result.IncrementalFleetWarmupMS = 0
	}
	coldSavedMS := findComparison(result.Comparisons, "cold-domain", "total").MainP50 - findComparison(result.Comparisons, "cold-domain", "total").BranchP50
	if coldSavedMS > 0 {
		result.LatencyBreakEvenDomains = result.IncrementalFleetWarmupMS / coldSavedMS
	}
	result.Checks = acceptanceChecks(result)
	for _, item := range result.Checks {
		result.Passed = result.Passed && item.Passed
	}
	return result, nil
}

func readInstallations(path string, expectedBlocks int) (map[string][]float64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 || !slices.Equal(records[0], []string{"block", "arm", "duration_ms"}) {
		return nil, errors.New("installation header must be block,arm,duration_ms")
	}
	result := map[string][]float64{"M": {}, "B": {}}
	seen := map[string]bool{}
	for index, record := range records[1:] {
		if len(record) != 3 || (record[1] != "M" && record[1] != "B") {
			return nil, fmt.Errorf("invalid installation row %d", index+2)
		}
		key := record[0] + "/" + record[1]
		if seen[key] {
			return nil, fmt.Errorf("duplicate installation row %s", key)
		}
		seen[key] = true
		duration, err := strconv.ParseFloat(record[2], 64)
		if err != nil || duration < 0 {
			return nil, fmt.Errorf("invalid installation duration in row %d", index+2)
		}
		result[record[1]] = append(result[record[1]], duration)
	}
	for _, arm := range []string{"M", "B"} {
		if len(result[arm]) != expectedBlocks {
			return nil, fmt.Errorf("arm %s has %d installation timings, want %d", arm, len(result[arm]), expectedBlocks)
		}
	}
	return result, nil
}

func stageStatistics(values []timeline) map[string]statistics {
	return map[string]statistics{
		"scheduler":   stats(valuesOf(values, func(v timeline) float64 { return v.SchedulerMS })),
		"nodePrepare": stats(valuesOf(values, func(v timeline) float64 { return v.NodePrepareMS })),
		"readiness":   stats(valuesOf(values, func(v timeline) float64 { return v.ReadinessMS })),
		"total":       stats(valuesOf(values, func(v timeline) float64 { return v.TotalMS })),
	}
}

func lifecycleStatistics(values []lifecycle) map[string]statistics {
	return map[string]statistics{
		"fence":        stats(lifecycleValues(values, func(v lifecycle) float64 { return v.FenceMS })),
		"finalization": stats(lifecycleValues(values, func(v lifecycle) float64 { return v.FinalizationMS })),
		"reuseReady":   stats(lifecycleValues(values, func(v lifecycle) float64 { return v.ReuseReadyMS })),
	}
}

func valuesOf(values []timeline, selectValue func(timeline) float64) []float64 {
	result := make([]float64, 0, len(values))
	for _, item := range values {
		result = append(result, selectValue(item))
	}
	return result
}

func lifecycleValues(values []lifecycle, selectValue func(lifecycle) float64) []float64 {
	result := make([]float64, 0, len(values))
	for _, item := range values {
		result = append(result, selectValue(item))
	}
	return result
}

func stats(values []float64) statistics {
	if len(values) == 0 {
		return statistics{}
	}
	ordered := slices.Clone(values)
	sort.Float64s(ordered)
	quarter := max(1, len(values)/4)
	first, last := slices.Clone(values[:quarter]), slices.Clone(values[len(values)-quarter:])
	sort.Float64s(first)
	sort.Float64s(last)
	firstP50, lastP50 := percentile(first, 0.50), percentile(last, 0.50)
	return statistics{
		Count: len(values), Minimum: ordered[0], P50: percentile(ordered, 0.50), P90: percentile(ordered, 0.90),
		P95: percentile(ordered, 0.95), Maximum: ordered[len(ordered)-1], FirstQuarter: firstP50,
		LastQuarter: lastP50, DriftRatio: ratio(lastP50, firstP50),
	}
}

func pairedBlockInterval(mainBlocks, branchBlocks map[int][]timeline, stage string, opts options) interval {
	deltas := make([]float64, 0, opts.expectedBlocks)
	for block := 1; block <= opts.expectedBlocks; block++ {
		mainValues := stageValues(mainBlocks[block], stage)
		branchValues := stageValues(branchBlocks[block], stage)
		if len(mainValues) == 0 || len(branchValues) == 0 {
			continue
		}
		deltas = append(deltas, median(branchValues)-median(mainValues))
	}
	if len(deltas) != opts.expectedBlocks {
		return interval{}
	}
	random := rand.New(rand.NewSource(opts.bootstrapSeed + int64(len(stage)))) // #nosec G404 -- deterministic statistical resampling.
	medians := make([]float64, 0, opts.bootstrapSamples)
	for range opts.bootstrapSamples {
		sample := make([]float64, len(deltas))
		for i := range sample {
			sample[i] = deltas[random.Intn(len(deltas))]
		}
		medians = append(medians, median(sample))
	}
	sort.Float64s(medians)
	return interval{Lower: percentile(medians, 0.025), Upper: percentile(medians, 0.975)}
}

func stageValues(values []timeline, stage string) []float64 {
	switch stage {
	case "scheduler":
		return valuesOf(values, func(v timeline) float64 { return v.SchedulerMS })
	case "nodePrepare":
		return valuesOf(values, func(v timeline) float64 { return v.NodePrepareMS })
	case "readiness":
		return valuesOf(values, func(v timeline) float64 { return v.ReadinessMS })
	default:
		return valuesOf(values, func(v timeline) float64 { return v.TotalMS })
	}
}

func acceptanceChecks(result report) []check {
	coldNode := findComparison(result.Comparisons, "cold-domain", "nodePrepare")
	coldTotal := findComparison(result.Comparisons, "cold-domain", "total")
	warmNode := findComparison(result.Comparisons, "warm-workload", "nodePrepare")
	warmTotal := findComparison(result.Comparisons, "warm-workload", "total")
	branchCold := findSession(result.Sessions, "B", "cold-domain")
	mainCold := findSession(result.Sessions, "M", "cold-domain")
	branchRetirement := branchCold.Lifecycle["reuseReady"]
	mainRetirement := mainCold.Lifecycle["reuseReady"]
	checks := []check{
		{Name: "cold-nodeprepare-at-least-20-percent-faster", Passed: coldNode.P50Ratio <= 0.80 && coldNode.P95Ratio <= 0.80 && coldNode.MedianDelta95.Upper < 0, Details: fmt.Sprintf("p50 ratio %.3f, p95 ratio %.3f, paired median delta CI [%0.1f,%0.1f]ms", coldNode.P50Ratio, coldNode.P95Ratio, coldNode.MedianDelta95.Lower, coldNode.MedianDelta95.Upper)},
		{Name: "cold-total-at-least-20-percent-faster", Passed: coldTotal.P50Ratio <= 0.80 && coldTotal.P95Ratio <= 0.80 && coldTotal.MedianDelta95.Upper < 0, Details: fmt.Sprintf("p50 ratio %.3f, p95 ratio %.3f, paired median delta CI [%0.1f,%0.1f]ms", coldTotal.P50Ratio, coldTotal.P95Ratio, coldTotal.MedianDelta95.Lower, coldTotal.MedianDelta95.Upper)},
		{Name: "warm-nodeprepare-no-more-than-5-percent-slower", Passed: warmNode.P50Ratio <= 1.05 && warmNode.P95Ratio <= 1.05, Details: fmt.Sprintf("p50 ratio %.3f, p95 ratio %.3f", warmNode.P50Ratio, warmNode.P95Ratio)},
		{Name: "warm-total-no-more-than-5-percent-slower", Passed: warmTotal.P50Ratio <= 1.05 && warmTotal.P95Ratio <= 1.05, Details: fmt.Sprintf("p50 ratio %.3f, p95 ratio %.3f", warmTotal.P50Ratio, warmTotal.P95Ratio)},
		{Name: "branch-cold-creates-no-per-domain-daemonset", Passed: branchCold.Churn.ByKind["daemonset"] == 0, Details: fmt.Sprintf("branch daemonset watch events %d; main %d", branchCold.Churn.ByKind["daemonset"], mainCold.Churn.ByKind["daemonset"])},
		{Name: "branch-retirement-no-more-than-20-percent-slower", Passed: ratio(branchRetirement.P95, mainRetirement.P95) <= 1.20, Details: fmt.Sprintf("D0-D4 p95 main %.1fms, branch %.1fms, ratio %.3f", mainRetirement.P95, branchRetirement.P95, ratio(branchRetirement.P95, mainRetirement.P95))},
		{Name: "finite-fleet-latency-break-even", Passed: coldTotal.MainP50 > coldTotal.BranchP50 && result.LatencyBreakEvenDomains >= 0, Details: fmt.Sprintf("incremental fleet warm-up %.1fms; break-even %.2f cold domains", result.IncrementalFleetWarmupMS, result.LatencyBreakEvenDomains)},
	}
	for _, session := range result.Sessions {
		for _, stage := range []string{"nodePrepare", "total"} {
			observed := session.Stages[stage]
			checks = append(checks, check{Name: strings.ToLower(session.Arm + "-" + session.Scenario + "-" + stage + "-drift"), Passed: observed.DriftRatio <= 1.10, Details: fmt.Sprintf("last/first quartile p50 ratio %.3f", observed.DriftRatio)})
		}
	}
	for _, scenario := range []string{"cold-domain", "warm-workload"} {
		mainSession, branchSession := findSession(result.Sessions, "M", scenario), findSession(result.Sessions, "B", scenario)
		checks = append(checks,
			check{Name: scenario + "-same-workload-image", Passed: len(mainSession.WorkloadImageIDs) > 0 && slices.Equal(mainSession.WorkloadImageIDs, branchSession.WorkloadImageIDs), Details: fmt.Sprintf("main=%v branch=%v", mainSession.WorkloadImageIDs, branchSession.WorkloadImageIDs)},
			check{Name: scenario + "-same-nodes", Passed: len(mainSession.Nodes) > 0 && slices.Equal(mainSession.Nodes, branchSession.Nodes), Details: fmt.Sprintf("main=%v branch=%v", mainSession.Nodes, branchSession.Nodes)},
		)
	}
	return checks
}

func validateLifecycleMeasurement(item lifecycle) error {
	if item.MeasurementVersion != watchReceiptMeasurementVersion {
		return fmt.Errorf("D0-D4 measurement version %q, want %s", item.MeasurementVersion, watchReceiptMeasurementVersion)
	}
	return nil
}

func readLifecycle(path string) ([]lifecycle, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var result []lifecycle
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var item lifecycle
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, scanner.Err()
}

func readChurn(root string) (churn, error) {
	result := churn{ByKind: map[string]int{}}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "-watch.json") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		kind := strings.TrimSuffix(entry.Name(), "-watch.json")
		count := len(watchEventRE.FindAll(data, -1))
		// A missing branch-only API on actual main leaves a kubectl error in the
		// redirected file. It is diagnostic evidence, not a serialized watch
		// event, and must not be charged to main's watch-byte total.
		if count > 0 {
			result.Bytes += int64(len(data))
		}
		result.Events += count
		result.ByKind[kind] += count
		return nil
	})
	return result, err
}

func readWorkloadIdentities(root string) ([]string, []string, error) {
	images, nodes := map[string]struct{}{}, map[string]struct{}{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != "pods.json" {
			return nil
		}
		var pods struct {
			Items []struct {
				Spec struct {
					NodeName string `json:"nodeName"`
				} `json:"spec"`
				Status struct {
					ContainerStatuses []struct {
						ImageID string `json:"imageID"`
					} `json:"containerStatuses"`
				} `json:"status"`
			} `json:"items"`
		}
		if err := readJSON(path, &pods); err != nil {
			return err
		}
		for _, pod := range pods.Items {
			if pod.Spec.NodeName != "" {
				nodes[pod.Spec.NodeName] = struct{}{}
			}
			for _, status := range pod.Status.ContainerStatuses {
				if status.ImageID != "" {
					images[status.ImageID] = struct{}{}
				}
			}
		}
		return nil
	})
	return sortedKeys(images), sortedKeys(nodes), err
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}

func findSession(values []armScenario, arm, scenario string) armScenario {
	for _, item := range values {
		if item.Arm == arm && item.Scenario == scenario {
			return item
		}
	}
	return armScenario{}
}

func findComparison(values []comparison, scenario, stage string) comparison {
	for _, item := range values {
		if item.Scenario == scenario && item.Stage == stage {
			return item
		}
	}
	return comparison{}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values))*p+0.999999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func median(values []float64) float64 {
	ordered := slices.Clone(values)
	sort.Float64s(ordered)
	return percentile(ordered, 0.50)
}

func ratio(numerator, denominator float64) float64 {
	if denominator == 0 {
		if numerator == 0 {
			return 1
		}
		return 1e308
	}
	return numerator / denominator
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func writeCSV(path string, result report) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"scenario", "stage", "main_p50_ms", "branch_p50_ms", "p50_ratio", "main_p95_ms", "branch_p95_ms", "p95_ratio", "paired_delta_ci_lower_ms", "paired_delta_ci_upper_ms"}); err != nil {
		return err
	}
	for _, item := range result.Comparisons {
		if err := writer.Write([]string{item.Scenario, item.Stage, number(item.MainP50), number(item.BranchP50), number(item.P50Ratio), number(item.MainP95), number(item.BranchP95), number(item.P95Ratio), number(item.MedianDelta95.Lower), number(item.MedianDelta95.Upper)}); err != nil {
			return err
		}
	}
	return writer.Error()
}

func writeMarkdown(path string, result report) error {
	var out strings.Builder
	fmt.Fprintf(&out, "# Main versus latest-branch two-Node performance report\n\n")
	fmt.Fprintf(&out, "Overall acceptance: **%s**\n\n", map[bool]string{true: "PASS", false: "FAIL"}[result.Passed])
	fmt.Fprintf(&out, "| Scenario | Stage | main p50 | branch p50 | ratio | main p95 | branch p95 | ratio | paired median delta 95%% CI |\n")
	fmt.Fprintf(&out, "|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, item := range result.Comparisons {
		fmt.Fprintf(&out, "| %s | %s | %.1f ms | %.1f ms | %.3f | %.1f ms | %.1f ms | %.3f | [%.1f, %.1f] ms |\n", item.Scenario, item.Stage, item.MainP50, item.BranchP50, item.P50Ratio, item.MainP95, item.BranchP95, item.P95Ratio, item.MedianDelta95.Lower, item.MedianDelta95.Upper)
	}
	fmt.Fprintf(&out, "\n## Acceptance checks\n\n")
	for _, item := range result.Checks {
		fmt.Fprintf(&out, "- [%s] %s — %s\n", map[bool]string{true: "x", false: " "}[item.Passed], item.Name, item.Details)
	}
	fmt.Fprintf(&out, "\n## D0–D4 retirement and exact-reuse evidence\n\n")
	fmt.Fprintf(&out, "| Arm | Fence p50/p95 | Finalization p50/p95 | Reuse-ready p50/p95/max |\n|---|---:|---:|---:|\n")
	for _, arm := range []string{"M", "B"} {
		session := findSession(result.Sessions, arm, "cold-domain")
		fence, finalization, reuse := session.Lifecycle["fence"], session.Lifecycle["finalization"], session.Lifecycle["reuseReady"]
		fmt.Fprintf(&out, "| %s | %.1f / %.1f ms | %.1f / %.1f ms | %.1f / %.1f / %.1f ms |\n", arm, fence.P50, fence.P95, finalization.P50, finalization.P95, reuse.P50, reuse.P95, reuse.Maximum)
	}
	fmt.Fprintf(&out, "\n## Installation amortization\n\n")
	fmt.Fprintf(&out, "Main installation p50: %.1f ms. Branch installation/fleet-ready p50: %.1f ms. Incremental fleet warm-up: %.1f ms. At the measured cold-domain p50 saving, latency breaks even after %.2f domains.\n", result.Installations["M"].P50, result.Installations["B"].P50, result.IncrementalFleetWarmupMS, result.LatencyBreakEvenDomains)
	fmt.Fprintf(&out, "\n## Client-visible watch churn\n\n")
	fmt.Fprintf(&out, "| Arm | Scenario | Events | Serialized bytes | DaemonSet events | Driver-Pod events |\n|---|---|---:|---:|---:|---:|\n")
	for _, session := range result.Sessions {
		fmt.Fprintf(&out, "| %s | %s | %d | %d | %d | %d |\n", session.Arm, session.Scenario, session.Churn.Events, session.Churn.Bytes, session.Churn.ByKind["daemonset"], session.Churn.ByKind["driver-pod"])
	}
	fmt.Fprintf(&out, "\n## Architectural attribution\n\n")
	fmt.Fprintf(&out, "The cold-domain comparison is expected to improve primarily in NodePrepare because the latest branch reuses an installed per-Node agent instead of creating and scheduling a per-ComputeDomain driver DaemonSet. The warm-workload comparison is the control: both arms already have their domain runtime, so a large remaining gap needs separate explanation.\n\n")
	fmt.Fprintf(&out, "This two-Node repeated-measures result does not replace the 18/144-Node promotion gate.\n")
	return os.WriteFile(path, []byte(out.String()), 0o644)
}

func number(value float64) string { return strconv.FormatFloat(value, 'f', 3, 64) }
