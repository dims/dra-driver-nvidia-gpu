//go:build controllerownedcdctest

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
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	testOnlyGPUCliqueAnnotation = "resource.nvidia.com/test-only-gpu-clique"
	testOnlyGPUCliqueEmpty      = "<empty>"
	testOnlyGPUCliqueError      = "<error>"
	testOnlyGPUCliqueGetTimeout = 10 * time.Second
)

// gpuCliqueIDProvider replaces NVML topology only in binaries built with the
// controllerownedcdctest tag. The Node annotation is persistent across plugin
// Pod replacement and Node reboot, which lets an e2e environment without an
// NVSwitch fabric exercise controller restart and retirement behavior without
// lying to the release binary about hardware state.
//
// A missing annotation delegates to NVML. The two sentinel values provide
// explicit negative-test inputs. No chart option or runtime flag can enable
// this provider in a release build.
func gpuCliqueIDProvider(config *Config, hardwareProvider func() (string, error)) func() (string, error) {
	return func() (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), testOnlyGPUCliqueGetTimeout)
		defer cancel()
		node, err := config.clientsets.Core.CoreV1().Nodes().Get(ctx, config.flags.nodeName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("TEST-ONLY synthetic GPU clique provider could not read Node %q: %w", config.flags.nodeName, err)
		}
		value, present := node.Annotations[testOnlyGPUCliqueAnnotation]
		if !present {
			return hardwareProvider()
		}
		klog.Warningf("TEST-ONLY synthetic GPU clique provider is active on Node %q with value %q; this binary must never be used for production topology", config.flags.nodeName, value)
		switch value {
		case "", testOnlyGPUCliqueEmpty:
			return "", nil
		case testOnlyGPUCliqueError:
			return "", fmt.Errorf("TEST-ONLY injected GPU clique discovery error")
		default:
			return value, nil
		}
	}
}
