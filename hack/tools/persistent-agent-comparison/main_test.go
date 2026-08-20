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
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPairedReportPassesAndAttributesDaemonSetChurn(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, "manifest.csv")
	file, err := os.Create(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"block", "arm", "scenario", "result", "lifecycle", "artifacts"}); err != nil {
		t.Fatal(err)
	}
	for block := 1; block <= 4; block++ {
		for _, arm := range []string{"M", "B"} {
			for _, scenario := range []string{"cold-domain", "warm-workload"} {
				directory := filepath.Join(root, fmt.Sprintf("%d-%s-%s", block, arm, scenario))
				if err := os.MkdirAll(directory, 0o755); err != nil {
					t.Fatal(err)
				}
				base := 1000.0
				if arm == "B" && scenario == "cold-domain" {
					base = 500
				}
				result := timelineResult{}
				for trial := 1; trial <= 25; trial++ {
					trialID := fmt.Sprintf("b%d-%s-%s-%03d", block, arm, scenario, trial)
					for pod := 0; pod < 2; pod++ {
						result.Timelines = append(result.Timelines, timeline{TrialID: trialID, SchedulerMS: 100, NodePrepareMS: base, ReadinessMS: 100, TotalMS: base + 200})
					}
				}
				resultPath := filepath.Join(directory, "result.json")
				if err := writeJSON(resultPath, result); err != nil {
					t.Fatal(err)
				}
				lifecyclePath := filepath.Join(directory, "lifecycle.jsonl")
				lifecycleFile, err := os.Create(lifecyclePath)
				if err != nil {
					t.Fatal(err)
				}
				encoder := json.NewEncoder(lifecycleFile)
				if scenario == "cold-domain" {
					for trial := 1; trial <= 25; trial++ {
						if err := encoder.Encode(lifecycle{MeasurementVersion: "watch-receipt-v1", TrialID: fmt.Sprintf("b%d-%s-cold-%03d", block, arm, trial), CycleClass: "measured", FenceMS: 1000, FinalizationMS: 1200, ReuseReadyMS: 1300}); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := lifecycleFile.Close(); err != nil {
					t.Fatal(err)
				}
				if arm == "M" && scenario == "cold-domain" {
					if err := os.WriteFile(filepath.Join(directory, "daemonset-watch.json"), []byte(`{"type":"ADDED"}`), 0o644); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(directory, "daemonset-watch-receipts.json"), []byte(`{"observedAtEpochMS":1,"type":"ADDED"}`), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(directory, "pods.json"), []byte(`{"items":[{"spec":{"nodeName":"node-a"},"status":{"containerStatuses":[{"imageID":"sha256:workload"}]}},{"spec":{"nodeName":"node-b"},"status":{"containerStatuses":[{"imageID":"sha256:workload"}]}}]}`), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := writer.Write([]string{fmt.Sprint(block), arm, scenario, resultPath, lifecyclePath, directory}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	installations := filepath.Join(root, "installations.csv")
	if err := os.WriteFile(installations, []byte("block,arm,duration_ms\n1,M,1000\n1,B,2000\n2,M,1000\n2,B,2000\n3,M,1000\n3,B,2000\n4,M,1000\n4,B,2000\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	output := filepath.Join(root, "output")
	opts := options{manifestPath: manifest, installationPath: installations, outputDir: output, expectedBlocks: 4, expectedTrials: 25, bootstrapSamples: 200, bootstrapSeed: 1, enforce: true}
	if err := run(opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	var got report
	if err := readJSON(filepath.Join(output, "comparison.json"), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Passed {
		t.Fatalf("report did not pass: %+v", got.Checks)
	}
	branch := findSession(got.Sessions, "B", "cold-domain")
	main := findSession(got.Sessions, "M", "cold-domain")
	if branch.Churn.ByKind["daemonset"] != 0 || main.Churn.ByKind["daemonset"] != 4 {
		t.Fatalf("unexpected daemonset churn: main=%+v branch=%+v", main.Churn, branch.Churn)
	}
	if _, err := os.Stat(filepath.Join(output, "report.md")); err != nil {
		t.Fatal(err)
	}
}

func TestManifestRejectsThirdArm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.csv")
	if err := os.WriteFile(path, []byte("block,arm,scenario,result,lifecycle,artifacts\n1,X,cold-domain,a,b,c\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(path); err == nil {
		t.Fatal("expected third arm to be rejected")
	}
}

func TestValidateLifecycleMeasurement(t *testing.T) {
	if err := validateLifecycleMeasurement(lifecycle{}); err == nil {
		t.Fatal("expected a polling-version lifecycle to be rejected")
	}
	if err := validateLifecycleMeasurement(lifecycle{MeasurementVersion: watchReceiptMeasurementVersion}); err != nil {
		t.Fatalf("watch-receipt lifecycle was rejected: %v", err)
	}
}
