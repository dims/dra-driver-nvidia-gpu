//go:build !controllerownedcdctest

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

// gpuCliqueIDProvider returns the hardware-backed topology source in release
// builds. The test-only alternative is compiled from
// gpu_clique_provider_controllerownedcdctest.go and cannot be enabled at
// runtime in a release binary.
func gpuCliqueIDProvider(_ *Config, hardwareProvider func() (string, error)) func() (string, error) {
	return hardwareProvider
}
