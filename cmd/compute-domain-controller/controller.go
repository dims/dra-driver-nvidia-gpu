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

// Package main implements a Kubernetes Device Resource Allocation (DRA) driver controller
package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/workqueue"
)

// ManagerConfig defines the common configuration options shared across all managers.
// It contains essential fields for driver identification, Kubernetes client access,
// and work queue management.
type ManagerConfig struct {
	// driverName is the unique identifier for this DRA driver
	driverName string

	// driverNamespace is the Kubernetes namespace where the driver operates
	driverNamespace string

	// imageName is the full image name to use when rendering templates
	imageName string

	// maxNodesPerIMEXDomain is the maximum number of nodes per IMEX domain to allocate
	maxNodesPerIMEXDomain int

	// imexConfig holds the resolved imex.mode / imex.isolation configuration.
	imexConfig imex.Config

	// clientsets provides access to various Kubernetes API client interfaces
	clientsets flags.ClientSets

	// workQueue manages the asynchronous processing of tasks
	workQueue *workqueue.WorkQueue

	// additionalNamespaces is a list of additional namespaces
	// where the driver can manage resources
	additionalNamespaces []string

	// logVerbosityCDDaemon controls the log verbosity for dynamically launched
	// ComputeDomain daemons.
	logVerbosityCDDaemon int

	// httpEndpoint is the TCP network address where the HTTP server for diagnostics
	// (including pprof and metrics) will listen
	httpEndpoint string

	// metricsPath is the HTTP path for Prometheus metrics
	metricsPath string

	// imagePullSecretNames are the names of the image pull secrets to apply to dynamically rendered compute-domain-daemon
	imagePullSecretNames []string

	// controllerOwnedCDCliquesAvailable is set only after the cluster-scoped
	// API preflight and the primary driver-namespace snapshot preflight pass.
	controllerOwnedCDCliquesAvailable bool
}

// Controller manages the lifecycle of the DRA driver and its components.
type Controller struct {
	// config holds the controller's configuration settings
	config *Config
}

// NewController creates and initializes a new Controller instance with the provided configuration.
func NewController(config *Config) *Controller {
	return &Controller{config: config}
}

// Run starts the controller's main loop and manages the lifecycle of its components.
// It initializes the work queue, starts the ComputeDomain manager, and handles
// graceful shutdown when the context is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	workQueue := workqueue.New(workqueue.DefaultControllerRateLimiter())

	// API support is independent of the feature gate. The gate chooses the
	// default for newly admitted ComputeDomains; persisted controller-v1
	// domains must keep reconciling after an operator disables that default.
	controllerOwnedAvailable := true
	if discovery := c.config.clientsets.Nvidia.Discovery(); discovery != nil {
		resources, err := discovery.ServerResourcesForGroupVersion("resource.nvidia.com/v1beta1")
		if err != nil {
			if featuregates.Enabled(featuregates.ControllerOwnedCDCliques) {
				klog.Errorf("controller-owned API discovery preflight failed: %v", err)
			}
			controllerOwnedAvailable = false
		} else if !apiResourcePresent(resources.APIResources, "computedomaincliquesnapshots/status") || !apiResourcePresent(resources.APIResources, "computedomaincliquereservations") {
			klog.Errorf("controller-owned API discovery is missing the snapshot status subresource or physical clique reservations")
			controllerOwnedAvailable = false
		}
	}
	if _, err := c.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		if featuregates.Enabled(featuregates.ControllerOwnedCDCliques) {
			klog.Errorf("controller-owned physical clique reservation API preflight failed; new ComputeDomains cannot use controller-v1: %v", err)
		}
		controllerOwnedAvailable = false
	}
	namespaces := append([]string{c.config.flags.namespace}, c.config.flags.additionalNamespaces.Value()...)
	namespaceAvailable := make(map[string]bool, len(namespaces))
	for _, namespace := range namespaces {
		if _, checked := namespaceAvailable[namespace]; checked {
			continue
		}
		_, err := c.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(namespace).List(ctx, metav1.ListOptions{Limit: 1})
		namespaceAvailable[namespace] = err == nil
		if err != nil {
			klog.Errorf("controller-owned clique API preflight failed in namespace %s; that namespace will not reconcile controller-v1 snapshots: %v", namespace, err)
			if namespace == c.config.flags.namespace {
				controllerOwnedAvailable = false
			}
		}
	}

	managerConfig := &ManagerConfig{
		driverName:                        c.config.driverName,
		driverNamespace:                   c.config.flags.namespace,
		additionalNamespaces:              c.config.flags.additionalNamespaces.Value(),
		imageName:                         c.config.flags.imageName,
		maxNodesPerIMEXDomain:             c.config.flags.maxNodesPerIMEXDomain,
		imexConfig:                        c.config.imexConfig,
		clientsets:                        c.config.clientsets,
		workQueue:                         workQueue,
		logVerbosityCDDaemon:              c.config.flags.logVerbosityCDDaemon,
		httpEndpoint:                      c.config.flags.httpEndpoint,
		metricsPath:                       c.config.flags.metricsPath,
		imagePullSecretNames:              c.config.imagePullSecretNames,
		controllerOwnedCDCliquesAvailable: controllerOwnedAvailable,
	}

	// TODO: log full, nested cliFlags structure.
	klog.Infof("controller manager config: %+v", managerConfig)

	cdManager := NewComputeDomainManager(managerConfig)
	var cliqueManagers []*ControllerOwnedCliqueManager
	if controllerOwnedAvailable {
		// The alpha protocol keeps snapshots and receipts in the primary driver
		// namespace. One manager also means one cluster-wide Node/CD watch rather
		// than duplicating those streams for each configured legacy namespace.
		cliqueManager := NewControllerOwnedCliqueManager(managerConfig)
		if err := cliqueManager.Start(ctx); err != nil {
			return fmt.Errorf("error starting controller-owned clique manager for namespace %s: %w", managerConfig.driverNamespace, err)
		}
		cliqueManagers = append(cliqueManagers, cliqueManager)
	}

	if err := cdManager.Start(ctx); err != nil {
		for _, cliqueManager := range cliqueManagers {
			cliqueManager.Stop()
		}
		return fmt.Errorf("error starting ComputeDomain manager: %w", err)
	}

	workQueue.Run(ctx)

	if err := cdManager.Stop(); err != nil {
		for _, cliqueManager := range cliqueManagers {
			cliqueManager.Stop()
		}
		return fmt.Errorf("error stopping ComputeDomain manager: %w", err)
	}
	for _, cliqueManager := range cliqueManagers {
		cliqueManager.Stop()
	}

	return nil
}

func apiResourcePresent(resources []metav1.APIResource, name string) bool {
	for i := range resources {
		if resources[i].Name == name {
			return true
		}
	}
	return false
}
