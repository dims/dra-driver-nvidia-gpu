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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
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

	// formationEventSink records low-frequency operator-action Events. It is
	// injected here rather than constructed by tests' deliberately narrow Core
	// clients, many of which implement only the Node and Pod APIs under test.
	formationEventSink func(context.Context, *nvapi.ComputeDomain, string, string) error
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
	requiresControllerOwned, err := controllerOwnedStateRequired(ctx, c.config)
	if err != nil {
		return err
	}
	if requiresControllerOwned {
		if err := validateControllerOwnedCDCInstallation(ctx, c.config); err != nil {
			return err
		}
	}

	// API support is independent of the feature gate. The gate chooses the
	// default for newly admitted ComputeDomains; persisted controller-v1
	// domains must keep reconciling after an operator disables that default.
	controllerOwnedAvailable := requiresControllerOwned
	if requiresControllerOwned {
		if discovery := c.config.clientsets.Nvidia.Discovery(); discovery != nil {
			resources, err := discovery.ServerResourcesForGroupVersion("resource.nvidia.com/v1beta1")
			if err != nil {
				klog.Errorf("controller-owned API discovery preflight failed: %v", err)
				controllerOwnedAvailable = false
			} else if !apiResourcePresent(resources.APIResources, "computedomaincliquesnapshots/status") ||
				!apiResourcePresent(resources.APIResources, "computedomaincliquereservations") ||
				!apiResourcePresent(resources.APIResources, "computedomaincliquereservations/status") ||
				!apiResourcePresent(resources.APIResources, "computedomaincliqueretirementevidences") {
				klog.Errorf("controller-owned API discovery is missing the snapshot, reservation, or retirement-evidence API")
				controllerOwnedAvailable = false
			}
		}
		if _, err := c.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			klog.Errorf("controller-owned physical clique reservation API preflight failed: %v", err)
			controllerOwnedAvailable = false
		}
		namespace := c.config.flags.namespace
		if _, err := c.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(namespace).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			klog.Errorf("controller-owned snapshot API preflight failed in namespace %s: %v", namespace, err)
			controllerOwnedAvailable = false
		}
		if _, err := c.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueRetirementEvidences(namespace).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
			klog.Errorf("controller-owned retirement-evidence API preflight failed in namespace %s: %v", namespace, err)
			controllerOwnedAvailable = false
		}
	}
	if requiresControllerOwned && !c.config.imexConfig.EffectiveHostManaged() && !controllerOwnedAvailable {
		return fmt.Errorf("claim-attested ComputeDomain routing and retirement require the reservation, snapshot, and retirement-evidence APIs; install and establish all three CRDs before rolling out controller and kubelet binaries")
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
	managerConfig.formationEventSink = func(ctx context.Context, cd *nvapi.ComputeDomain, reason, message string) error {
		now := metav1.Now()
		_, err := c.config.clientsets.Core.CoreV1().Events(cd.Namespace).Create(ctx, &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "computedomain-isolation-", Namespace: cd.Namespace},
			InvolvedObject: corev1.ObjectReference{
				APIVersion: nvapi.SchemeGroupVersion.String(), Kind: nvapi.ComputeDomainKind,
				Namespace: cd.Namespace, Name: cd.Name, UID: cd.UID,
			},
			Reason: reason, Message: message, Type: corev1.EventTypeWarning,
			Source:         corev1.EventSource{Component: "compute-domain-controller"},
			FirstTimestamp: now, LastTimestamp: now, Count: 1,
		}, metav1.CreateOptions{})
		return err
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
		cdManager.controllerOwnedCliqueManager = cliqueManager
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

// controllerOwnedStateRequired separates the alpha protocol's durable runtime
// requirements from the legacy default. The feature gate admits new streams;
// persisted protocol markers or reservations keep the controller active after
// that default is disabled. A legacy-only installation does not require the
// alpha CRDs, installation marker, topology publishing, or leader election.
func controllerOwnedStateRequired(ctx context.Context, config *Config) (bool, error) {
	if featuregates.Enabled(featuregates.ControllerOwnedCDCliques) {
		return true, nil
	}
	computeDomains, err := config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("check persisted ComputeDomain protocols: %w", err)
	}
	for i := range computeDomains.Items {
		if computeDomains.Items[i].Annotations[nvapi.ComputeDomainCliqueProtocolAnnotation] == string(nvapi.ComputeDomainCliqueProtocolControllerV1) {
			return true, nil
		}
	}
	reservations, err := config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(ctx, metav1.ListOptions{Limit: 1})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check persisted controller-owned clique reservations: %w", err)
	}
	return len(reservations.Items) > 0, nil
}

func validateControllerOwnedCDCInstallation(ctx context.Context, config *Config) error {
	installation, err := config.clientsets.Core.RbacV1().ClusterRoles().Get(ctx, controllerOwnedCDCInstallationPolicyName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("controller-owned CDC installation guard %q is missing; install the chart admission policies before starting the controller", controllerOwnedCDCInstallationPolicyName)
		}
		return fmt.Errorf("read controller-owned CDC installation guard %q: %w", controllerOwnedCDCInstallationPolicyName, err)
	}
	expectedSubject := fmt.Sprintf("system:serviceaccount:%s:%s", config.flags.namespace, config.flags.serviceAccountName)
	return validateControllerOwnedCDCInstallationIdentity(installation.Annotations, config.flags.installationID, config.flags.namespace, expectedSubject)
}

func validateControllerOwnedCDCInstallationIdentity(annotations map[string]string, installationID, namespace, controllerSubject string) error {
	checks := []struct {
		annotation  string
		expected    string
		description string
	}{
		{controllerOwnedCDCInstallationAnnotation, installationID, "Helm installation"},
		{controllerOwnedCDCControlNamespaceAnnotation, namespace, "control namespace"},
		{controllerOwnedCDCControllerSubjectAnnotation, controllerSubject, "controller ServiceAccount"},
	}
	for _, check := range checks {
		if actual := annotations[check.annotation]; actual != check.expected {
			return fmt.Errorf("controller-owned CDC single-install preflight rejected this process: %s in installation guard %q is %q, expected %q", check.description, controllerOwnedCDCInstallationPolicyName, actual, check.expected)
		}
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
