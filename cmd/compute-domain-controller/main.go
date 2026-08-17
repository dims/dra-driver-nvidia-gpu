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

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/urfave/cli/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	_ "k8s.io/component-base/metrics/prometheus/restclient" // for client metric registration
	_ "k8s.io/component-base/metrics/prometheus/version"    // for version metric registration
	_ "k8s.io/component-base/metrics/prometheus/workqueue"  // register work queues in the default legacy registry

	"sigs.k8s.io/dra-driver-nvidia-gpu/internal/common"
	"sigs.k8s.io/dra-driver-nvidia-gpu/internal/info"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
	pkgflags "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
)

const (
	DriverName                                 = "compute-domain.nvidia.com"
	persistentAgentInstallationPolicyName      = "persistent-agent-installation.dra-driver-nvidia-gpu"
	persistentAgentInstallationAnnotation      = "resource.nvidia.com/persistent-agent-installation"
	persistentAgentControlNamespaceAnnotation  = "resource.nvidia.com/persistent-agent-control-namespace"
	persistentAgentControllerSubjectAnnotation = "resource.nvidia.com/persistent-agent-controller-subject"

	// This constant provides a reasonable default for the maximum size of
	// a given IMEX Domain. On GB200 and GB300 the limit is 18, so we pick
	// this for now. It can be overridden as an environment variable or
	// command line argument as required.
	defaultMaxNodesPerIMEXDomain = 18
)

type Flags struct {
	kubeClientConfig     pkgflags.KubeClientConfig
	leaderElectionConfig pkgflags.LeaderElectionConfig

	podName               string
	namespace             string
	serviceAccountName    string
	installationID        string
	imageName             string
	maxNodesPerIMEXDomain int
	logVerbosityCDDaemon  int

	imexMode      string
	imexIsolation string

	httpEndpoint string
	metricsPath  string
	profilePath  string

	additionalNamespaces cli.StringSlice
	imagePullSecretsCSV  string
	klogVerbosity        int
}

// trackingResourceLock records local acquisition history independently of the
// LeaderElector's current observed holder. After this process observes a
// successor, IsLeader/GetLeader are false even though its leader callback may
// still be draining. Update/Create are the atomic points at which this
// identity actually acquires or renews the lock.
type trackingResourceLock struct {
	resourcelock.Interface
	identity string
	acquired chan struct{}
	once     sync.Once
}

func (l *trackingResourceLock) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Create(ctx, record)
	if err == nil && record.HolderIdentity == l.identity {
		l.once.Do(func() { close(l.acquired) })
	}
	return err
}

func (l *trackingResourceLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Update(ctx, record)
	if err == nil && record.HolderIdentity == l.identity {
		l.once.Do(func() { close(l.acquired) })
	}
	return err
}

type Config struct {
	driverName           string
	flags                *Flags
	clientsets           pkgflags.ClientSets
	mux                  *http.ServeMux
	imagePullSecretNames []string
	imexConfig           imex.Config
}

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newApp() *cli.App {
	loggingConfig := pkgflags.NewLoggingConfig()
	featureGateConfig := pkgflags.NewFeatureGateConfig()
	flags := &Flags{}

	cliFlags := []cli.Flag{
		&cli.StringFlag{
			Name:        "pod-name",
			Usage:       "The name of the pod this controller is running in.",
			Required:    true,
			Destination: &flags.podName,
			EnvVars:     []string{"POD_NAME"},
		},
		&cli.StringFlag{
			Name:        "service-account-name",
			Usage:       "The ServiceAccount name used by this controller Pod.",
			Destination: &flags.serviceAccountName,
			EnvVars:     []string{"SERVICE_ACCOUNT_NAME"},
		},
		&cli.StringFlag{
			Name:        "persistent-agent-installation-id",
			Usage:       "Immutable Helm release identity which owns persistent-agent admission.",
			Destination: &flags.installationID,
			EnvVars:     []string{"PERSISTENT_AGENT_INSTALLATION_ID"},
		},
		&cli.StringFlag{
			Name:        "namespace",
			Usage:       "The namespace of the pod this controller is running in.",
			Value:       "default",
			Destination: &flags.namespace,
			EnvVars:     []string{"NAMESPACE"},
		},
		&cli.StringFlag{
			Name:        "image-name",
			Usage:       "The full image name to use for rendering templates.",
			Required:    true,
			Destination: &flags.imageName,
			EnvVars:     []string{"IMAGE_NAME"},
		},
		&cli.StringFlag{
			Name:        "cd-daemon-image-pull-secret-names",
			Usage:       "Comma-separated imagePullSecret names for compute-domain-daemon DaemonSets (e.g. regcred,other). Empty string means none.",
			Destination: &flags.imagePullSecretsCSV,
			EnvVars:     []string{"CD_DAEMON_IMAGE_PULL_SECRET_NAMES"},
		},
		&cli.IntFlag{
			Name:        "log-verbosity-cd-daemon",
			Usage:       "Log verbosity for dynamically launched CD daemon pods",
			Required:    true,
			EnvVars:     []string{"LOG_VERBOSITY_CD_DAEMON"},
			Destination: &flags.logVerbosityCDDaemon,
		},
		&cli.IntFlag{
			Name:        "max-nodes-per-imex-domain",
			Usage:       "The maximum number of possible nodes per IMEX domain",
			Value:       defaultMaxNodesPerIMEXDomain,
			EnvVars:     []string{"MAX_NODES_PER_IMEX_DOMAIN"},
			Destination: &flags.maxNodesPerIMEXDomain,
		},
		&cli.StringFlag{
			Name:        "imex-mode",
			Usage:       "IMEX deployment mode: driverManaged or hostManaged.",
			Value:       string(imex.ModeDriverManaged),
			Destination: &flags.imexMode,
			EnvVars:     []string{"IMEX_MODE"},
		},
		&cli.StringFlag{
			Name:        "imex-isolation",
			Usage:       "IMEX isolation strategy: domain (default; all workloads in the same IMEX domain share channel 0) or channel (not yet supported).",
			Value:       string(imex.IsolationIMEXDomain),
			Destination: &flags.imexIsolation,
			EnvVars:     []string{"IMEX_ISOLATION"},
		},
		&cli.StringFlag{
			Category:    "HTTP server:",
			Name:        "http-endpoint",
			Usage:       "The TCP network `address` where the HTTP server for diagnostics, including pprof and metrics will listen (example: `:8080`). The default is the empty string, which means the server is disabled.",
			Destination: &flags.httpEndpoint,
			EnvVars:     []string{"HTTP_ENDPOINT"},
		},
		&cli.StringFlag{
			Category:    "HTTP server:",
			Name:        "metrics-path",
			Usage:       "The HTTP `path` where Prometheus metrics will be exposed, disabled if empty.",
			Value:       "/metrics",
			Destination: &flags.metricsPath,
			EnvVars:     []string{"METRICS_PATH"},
		},
		&cli.StringFlag{
			Category:    "HTTP server:",
			Name:        "pprof-path",
			Usage:       "The HTTP `path` where pprof profiling will be available, disabled if empty.",
			Destination: &flags.profilePath,
			EnvVars:     []string{"PPROF_PATH"},
		},
		&cli.StringSliceFlag{
			Name:        "additional-namespaces",
			Usage:       "Additional namespaces where the driver can manage resources.",
			Destination: &flags.additionalNamespaces,
			EnvVars:     []string{"ADDITIONAL_NAMESPACES"},
		},
	}

	cliFlags = append(cliFlags, flags.leaderElectionConfig.Flags()...)
	cliFlags = append(cliFlags, flags.kubeClientConfig.Flags()...)
	cliFlags = append(cliFlags, featureGateConfig.Flags()...)
	cliFlags = append(cliFlags, loggingConfig.Flags()...)

	app := &cli.App{
		Name:            "compute-domain-controller",
		Usage:           "compute-domain-controller implements a DRA driver controller for NVIDIA compute domains.",
		ArgsUsage:       " ",
		HideHelpCommand: true,
		Flags:           cliFlags,
		Before: func(c *cli.Context) error {
			if c.Args().Len() > 0 {
				return fmt.Errorf("arguments not supported: %v", c.Args().Slice())
			}
			if flags.maxNodesPerIMEXDomain < 1 || flags.maxNodesPerIMEXDomain > 1024 {
				return fmt.Errorf("max-nodes-per-imex-domain must be between 1 and 1024")
			}
			// `loggingConfig` must be applied before doing any logging
			err := loggingConfig.Apply()

			// Store klog's log verbosity setting in this program's config for
			// later runtime inspection (it's otherwise not accessible anymore
			// because we do not expose the raw `cliFlags`).
			flags.klogVerbosity = int(loggingConfig.Config.Verbosity)
			pkgflags.LogStartupConfig(flags, loggingConfig)
			return err
		},
		Action: func(c *cli.Context) error {
			common.StartDebugSignalHandlers()

			imexConfig := imex.Config{Mode: imex.Mode(flags.imexMode), Isolation: imex.Isolation(flags.imexIsolation)}
			if err := imexConfig.Validate(featuregates.Enabled(featuregates.HostManagedIMEXDaemon)); err != nil {
				return fmt.Errorf("imex configuration validation failed: %w", err)
			}
			if imexConfig.EffectiveHostManaged() {
				// The driver never creates per-ComputeDomain IMEX DaemonSets or
				// ComputeDomainClique objects in host-managed mode, so these gates
				// are meaningless (and their defaults would otherwise conflict).
				if err := featuregates.FeatureGates().SetFromMap(map[string]bool{
					string(featuregates.IMEXDaemonsWithDNSNames):       false,
					string(featuregates.ComputeDomainCliques):          false,
					string(featuregates.PersistentComputeDomainAgents): false,
				}); err != nil {
					return fmt.Errorf("error forcing feature gates for hostManaged IMEX: %w", err)
				}
			}
			// Validate after host-managed normalization so an explicitly enabled
			// persistent-agent gate is safely forced off with its dependencies.
			if err := featuregates.ValidateFeatureGates(); err != nil {
				return fmt.Errorf("feature gate validation failed: %w", err)
			}
			mux := http.NewServeMux()

			clientsets, err := flags.kubeClientConfig.NewClientSets()
			if err != nil {
				return fmt.Errorf("create client: %w", err)
			}
			config := &Config{
				mux:                  mux,
				flags:                flags,
				clientsets:           clientsets,
				driverName:           DriverName,
				imagePullSecretNames: strings.Fields(strings.ReplaceAll(strings.TrimSpace(flags.imagePullSecretsCSV), ",", " ")),
				imexConfig:           imexConfig,
			}
			persistentStateRequired, err := persistentAgentStateRequired(c.Context, config)
			if err != nil {
				return err
			}
			if imexConfig.EffectiveHostManaged() && persistentStateRequired {
				return fmt.Errorf("imex.mode=hostManaged is unsafe while persistent-agent state exists; retire it first")
			}
			if !flags.leaderElectionConfig.Enabled && persistentStateRequired {
				return fmt.Errorf("leader election is required while persistent-agent state exists")
			}

			if flags.httpEndpoint != "" {
				err = SetupHTTPEndpoint(config)
				if err != nil {
					return fmt.Errorf("create http endpoint: %w", err)
				}
			}

			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

			errChan := make(chan error, 1)
			controller := NewController(config)
			ctx, cancel := context.WithCancel(c.Context)
			go func() {
				// Fallback to standalone mode if leader election is disabled
				if !config.flags.leaderElectionConfig.Enabled {
					klog.Info("Leader election disabled, starting controller directly")
					errChan <- controller.Run(ctx)
					return
				}
				errChan <- runWithLeaderElection(ctx, config, controller)
			}()

			for {
				select {
				case sig := <-sigs:
					klog.InfoS("Received signal, shutting down", "signal", sig.String())
					cancel()
				case err := <-errChan:
					cancel()
					if err != nil {
						return fmt.Errorf("run controller: %w", err)
					}
					return nil
				}
			}
		},
		After: func(c *cli.Context) error {
			// Runs after `Action` (regardless of success/error). In urfave cli
			// v2, the final error reported will be from either Action, Before,
			// or After (whichever is non-nil and last executed).
			klog.Infof("shutdown")
			logs.FlushLogs()
			return nil
		},
		Version: info.GetVersionString(),
	}

	// We remove the -v alias for the version flag so as to not conflict with the -v flag used for klog.
	f, ok := cli.VersionFlag.(*cli.BoolFlag)
	if ok {
		f.Aliases = nil
	}

	return app
}

func runWithLeaderElection(ctx context.Context, config *Config, controller *Controller) error {
	klog.Info("Leader election enabled")
	// Unique identity: PodName + UUID to prevent conflicts on restarts
	id := uuid.New().String()
	lockID := fmt.Sprintf("%s-%s", config.flags.podName, id)
	klog.InfoS("Leader election candidate registered", "lockID", lockID,
		"leaseName", config.flags.leaderElectionConfig.LeaseLockName,
		"leaseNamespace", config.flags.leaderElectionConfig.LeaseLockNamespace)

	// electorCtx controls the lifecycle of the leader election loop
	electorCtx, cancelElector := context.WithCancel(ctx)
	// Standard defer to ensure resources are cleaned up on function exit
	defer cancelElector()

	leaseLock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      config.flags.leaderElectionConfig.LeaseLockName,
			Namespace: config.flags.leaderElectionConfig.LeaseLockNamespace,
		},
		Client: config.clientsets.Core.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: lockID,
		},
	}
	lock := &trackingResourceLock{
		Interface: leaseLock,
		identity:  lockID,
		acquired:  make(chan struct{}),
	}

	controllerErrCh := make(chan error, 1)
	controllerStartedCh := make(chan struct{})
	controllerDoneCh := make(chan struct{})
	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(leaderCtx context.Context) {
			close(controllerStartedCh)
			defer close(controllerDoneCh)
			klog.InfoS("Became leader, starting controller", "lockID", lockID)

			// ARCHITECTURE NOTE:
			// We use cancelElector() to ensure that if the controller logic exits
			// (either gracefully or with an error), the entire leader election loop
			// terminates. The Lease then expires conservatively after this callback's
			// guarded workers have drained.
			//
			// By returning from run() after elector.Run() finishes, we rely on
			// Kubernetes to restart the Pod, ensuring a clean in-memory state
			// for the next leadership term.
			defer cancelElector()

			// NOTE: Use leaderCtx provided by the callback.
			// It is automatically cancelled if leadership is lost.
			if err := controller.Run(leaderCtx); err != nil {
				select {
				case controllerErrCh <- err:
				default:
				}
				klog.ErrorS(err, "Controller exited with error", "lockID", lockID)
			} else {
				klog.InfoS("Controller exited gracefully", "lockID", lockID)
			}
		},
		OnStoppedLeading: func() {
			// ARCHITECTURE NOTE:
			// We only log here. The actual shutdown of the controller is handled by the
			// cancellation of the leaderCtx passed to OnStartedLeading.
			// When leadership is lost, the library cancels that context, triggering
			// the controller's graceful shutdown logic.
			klog.Warningf("Stopped leading, lockID: %s", lockID)
		},
		OnNewLeader: func(identity string) {
			// OnNewLeader is called when a new leader is observed.
			// We ignore the case where the "new" leader is ourselves to avoid
			// redundant logs during initial election or re-election.
			if identity == lockID {
				klog.V(6).InfoS("OnNewLeader callback: observed leader is still ourselves", "lockID", lockID)
				return
			}
			klog.InfoS("New leader elected", "leader", identity, "currentCandidate", lockID)
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: config.flags.leaderElectionConfig.LeaseDuration,
		RenewDeadline: config.flags.leaderElectionConfig.RenewDeadline,
		RetryPeriod:   config.flags.leaderElectionConfig.RetryPeriod,
		Name:          config.flags.leaderElectionConfig.LeaseLockName,
		Callbacks:     callbacks,
		// The leader callback performs durable allocation writes and owns worker
		// goroutines. Do not clear the Lease merely because its context was
		// cancelled: client-go requires all guarded work to be proven stopped
		// before ReleaseOnCancel is safe. Lease expiry provides conservative
		// handoff while the callback drains.
		ReleaseOnCancel: false,
	})
	if err != nil {
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	// Block until electorCtx is cancelled or leadership is lost
	klog.InfoS("Starting leader election loop", "lockID", lockID)
	elector.Run(electorCtx)
	// LeaderElector.Run launches OnStartedLeading asynchronously. Join that
	// callback explicitly so this process never returns from leader election
	// while allocation workers from the old leadership term can still write.
	// Current observed holder is not acquisition history: after observing a
	// successor IsLeader/GetLeader may be false while our callback still drains.
	// The tracking lock closes acquired on the successful Create/Update that
	// made this identity leader. LeaderElector then schedules OnStartedLeading
	// before entering renew, so join both scheduling and callback completion.
	select {
	case <-lock.acquired:
		<-controllerStartedCh
		<-controllerDoneCh
	default:
	}

	// If exiting due to a controller failure, propagate the error to main
	select {
	case err := <-controllerErrCh:
		if err != nil {
			klog.ErrorS(err, "Process exiting due to controller failure")
			return fmt.Errorf("controller execution failed: %w", err)
		}
	default:
	}
	klog.InfoS("Leader election loop ended gracefully", "lockID", lockID)
	return nil
}

func SetupHTTPEndpoint(config *Config) error {
	if config.flags.metricsPath != "" {
		actualPath := path.Join("/", config.flags.metricsPath)
		klog.InfoS("Starting metrics", "path", actualPath)
		config.mux.Handle(path.Join("/", config.flags.metricsPath), metrics.NewLegacyPrometheusHandler())
	}

	if config.flags.profilePath != "" {
		actualPath := path.Join("/", config.flags.profilePath)
		klog.InfoS("Starting profiling", "path", actualPath)
		config.mux.HandleFunc(actualPath, pprof.Index)
		config.mux.HandleFunc(path.Join(actualPath, "cmdline"), pprof.Cmdline)
		config.mux.HandleFunc(path.Join(actualPath, "profile"), pprof.Profile)
		config.mux.HandleFunc(path.Join(actualPath, "symbol"), pprof.Symbol)
		config.mux.HandleFunc(path.Join(actualPath, "trace"), pprof.Trace)
	}

	listener, err := net.Listen("tcp", config.flags.httpEndpoint)
	if err != nil {
		return fmt.Errorf("listen on HTTP endpoint: %w", err)
	}

	go func() {
		klog.InfoS("Starting HTTP server", "endpoint", config.flags.httpEndpoint)
		err := http.Serve(listener, config.mux)
		if err != nil {
			klog.ErrorS(err, "HTTP server failed")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}()

	return nil
}
