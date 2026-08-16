{{/*
Expand the name of the chart.
*/}}
{{- define "dra-driver-nvidia-gpu.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dra-driver-nvidia-gpu.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Allow the release namespace to be overridden for multi-namespace deployments in combined charts
*/}}
{{- define "dra-driver-nvidia-gpu.namespace" -}}
  {{- if .Values.namespaceOverride -}}
    {{- .Values.namespaceOverride -}}
  {{- else -}}
    {{- .Release.Namespace -}}
  {{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dra-driver-nvidia-gpu.chart" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" $name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Standard labels: documented at
https://helm.sh/docs/chart_best_practices/labels/
Apply this to all high-level objects (Deployment, DaemonSet, ...).
Pod template labels are included here to deliver name+instance.
*/}}
{{- define "dra-driver-nvidia-gpu.labels" -}}
helm.sh/chart: {{ include "dra-driver-nvidia-gpu.chart" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{ include "dra-driver-nvidia-gpu.templateLabels" . }}
{{- end }}

{{/*
Apply this to all pod templates (a smaller set of labels compared to
the set of standard labels above, to not clutter individual pods too
much). Note that these labels cannot be used to distinguish
components within this Helm chart.
*/}}
{{- define "dra-driver-nvidia-gpu.templateLabels" -}}
app.kubernetes.io/name: {{ include "dra-driver-nvidia-gpu.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector label: precisely filter for just the pods of the corresponding
Deployment, DaemonSet, .... That is, this label key/value pair must be
different per-component (a component name is a required argument). This
could be many labels, but we want to use just one (with a sufficiently
unique key).

TOOD: remove the override feature, or make the override work per-component.
*/}}
{{- define "dra-driver-nvidia-gpu.selectorLabels" -}}
{{- if and (hasKey . "componentName") (hasKey . "context") -}}
{{- if .context.Values.selectorLabelsOverride -}}
{{ toYaml .context.Values.selectorLabelsOverride }}
{{- else -}}
{{- $name := default .context.Chart.Name .context.Values.nameOverride -}}
{{ $name }}-component: {{ .componentName }}
{{- end }}
{{- else -}}
fail "selectorLabels: both arguments are required: context, componentName"
{{- end }}
{{- end }}

{{/*
Full image name with tag
*/}}
{{- define "dra-driver-nvidia-gpu.fullimage" -}}
{{- $tag := printf "v%s" .Chart.AppVersion }}
{{- .Values.image.repository -}}:{{- .Values.image.tag | default $tag -}}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "dra-driver-nvidia-gpu.serviceAccountName" -}}
{{- $name := printf "%s-service-account" (include "dra-driver-nvidia-gpu.fullname" .) }}
{{- if .Values.serviceAccount.create }}
{{- default $name .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Controller-owned CDC admission is cluster-scoped. Its names and authenticated
subjects must not vary with nameOverride/fullnameOverride: doing so would make
two releases either overwrite one another or install conjunctive policies.
*/}}
{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCInstallationName" -}}
controller-owned-cdc-installation.dra-driver-nvidia-gpu
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCInstallationID" -}}
{{- printf "%s/%s" (include "dra-driver-nvidia-gpu.namespace" . | trim) .Release.Name -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCControllerRoleName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}
dra-driver-nvidia-gpu-clusterrole-controller
{{- else -}}
{{- printf "%s-clusterrole-controller" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletRoleName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}
dra-driver-nvidia-gpu-clusterrole-kubeletplugin
{{- else -}}
{{- printf "%s-clusterrole-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCControllerBindingName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}
dra-driver-nvidia-gpu-clusterrole-binding-controller
{{- else -}}
{{- printf "%s-clusterrole-binding-controller-%s" (include "dra-driver-nvidia-gpu.name" .) (include "dra-driver-nvidia-gpu.namespace" .) -}}
{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletBindingName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}
dra-driver-nvidia-gpu-clusterrole-binding-kubeletplugin
{{- else -}}
{{- printf "%s-clusterrole-binding-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCDaemonReaderRoleName" -}}
dra-driver-nvidia-gpu-clusterrole-daemon-reader
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCDaemonReaderBindingName" -}}
dra-driver-nvidia-gpu-clusterrole-binding-daemon-reader
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCControllerWorkloadName" -}}
{{- printf "%s-controller" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletWorkloadName" -}}
{{- printf "%s-kubelet-plugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCControllerNamespaceRoleName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}dra-driver-nvidia-gpu-role-controller{{- else -}}{{- printf "%s-role-controller" (include "dra-driver-nvidia-gpu.name" .) -}}{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCControllerNamespaceBindingName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}dra-driver-nvidia-gpu-role-binding-controller{{- else -}}{{- printf "%s-role-binding-controller" (include "dra-driver-nvidia-gpu.name" .) -}}{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletNamespaceRoleName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}dra-driver-nvidia-gpu-role-kubeletplugin{{- else -}}{{- printf "%s-role-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}{{- end -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletNamespaceBindingName" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled -}}dra-driver-nvidia-gpu-role-binding-kubeletplugin{{- else -}}{{- printf "%s-role-binding-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}{{- end -}}
{{- end -}}

{{/*
Names emitted by charts which predate controller-owned CDC admission. The
immutable installation marker records these aliases so a verified zero-state
rollback can restore the old workloads and RBAC without opening the binding
policies to a second release or control namespace.
*/}}
{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerRoleName" -}}
{{- printf "%s-clusterrole-controller" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletRoleName" -}}
{{- printf "%s-clusterrole-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerBindingName" -}}
{{- printf "%s-clusterrole-binding-controller-%s" (include "dra-driver-nvidia-gpu.name" .) (include "dra-driver-nvidia-gpu.namespace" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletBindingName" -}}
{{- printf "%s-clusterrole-binding-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerNamespaceRoleName" -}}
{{- printf "%s-role-controller" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerNamespaceBindingName" -}}
{{- printf "%s-role-binding-controller" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletNamespaceRoleName" -}}
{{- printf "%s-role-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletNamespaceBindingName" -}}
{{- printf "%s-role-binding-kubeletplugin" (include "dra-driver-nvidia-gpu.name" .) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerServiceAccountUsername" -}}
{{- printf "system:serviceaccount:%s:%s-controller" (include "dra-driver-nvidia-gpu.namespace" . | trim) (include "dra-driver-nvidia-gpu.serviceAccountName" . | trim) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.kubeletPluginServiceAccountUsername" -}}
{{- printf "system:serviceaccount:%s:%s-kubeletplugin" (include "dra-driver-nvidia-gpu.namespace" . | trim) (include "dra-driver-nvidia-gpu.serviceAccountName" . | trim) -}}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCMarkerAnnotations" -}}
helm.sh/resource-policy: keep
meta.helm.sh/release-name: {{ .Release.Name | quote }}
meta.helm.sh/release-namespace: {{ .Release.Namespace | quote }}
resource.nvidia.com/controller-owned-cdc-installation: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCInstallationID" . | quote }}
resource.nvidia.com/controller-owned-cdc-control-namespace: {{ include "dra-driver-nvidia-gpu.namespace" . | trim | quote }}
resource.nvidia.com/controller-owned-cdc-controller-subject: {{ include "dra-driver-nvidia-gpu.controllerServiceAccountUsername" . | quote }}
resource.nvidia.com/controller-owned-cdc-kubelet-subject: {{ include "dra-driver-nvidia-gpu.kubeletPluginServiceAccountUsername" . | quote }}
resource.nvidia.com/controller-owned-cdc-controller-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCControllerRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-kubelet-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-controller-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCControllerBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-kubelet-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-daemon-role: "compute-domain-daemon-role"
resource.nvidia.com/controller-owned-cdc-legacy-daemon-binding: "compute-domain-daemon-role-binding"
resource.nvidia.com/controller-owned-cdc-controller-workload: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCControllerWorkloadName" . | quote }}
resource.nvidia.com/controller-owned-cdc-kubelet-workload: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletWorkloadName" . | quote }}
resource.nvidia.com/controller-owned-cdc-controller-namespace-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCControllerNamespaceRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-controller-namespace-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCControllerNamespaceBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-kubelet-namespace-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletNamespaceRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-kubelet-namespace-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCKubeletNamespaceBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-controller-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-kubelet-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-controller-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-kubelet-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-controller-namespace-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerNamespaceRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-controller-namespace-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyControllerNamespaceBindingName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-kubelet-namespace-role: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletNamespaceRoleName" . | quote }}
resource.nvidia.com/controller-owned-cdc-legacy-kubelet-namespace-binding: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCLegacyKubeletNamespaceBindingName" . | quote }}
{{- end -}}

{{/*
Safety policies and bindings are deliberately release-neutral objects. Helm's
live install adds its ownership metadata after the admission-first bootstrap;
an offline render must not contain metadata with which another release could
claim those retained objects.
*/}}
{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCAnnotations" -}}
helm.sh/resource-policy: keep
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCObjectIdentity" -}}
{{- if .Values.controllerOwnedCDCliques.admissionEnabled }}
resource.nvidia.com/controller-owned-cdc-installation: {{ include "dra-driver-nvidia-gpu.controllerOwnedCDCInstallationID" . | quote }}
{{- end }}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCLabels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{- define "dra-driver-nvidia-gpu.controllerOwnedCDCPolicyName" -}}
{{- printf "%s-dra-driver-nvidia-gpu" . -}}
{{- end -}}

{{/*
Create the name of the webhook service account to use
*/}}
{{- define "dra-driver-nvidia-gpu.webhookServiceAccountName" -}}
{{- $name := printf "%s-webhook-service-account" (include "dra-driver-nvidia-gpu.fullname" .) }}
{{- if .Values.webhook.serviceAccount.create }}
{{- default $name .Values.webhook.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.webhook.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Check for the existence of an element in a list
*/}}
{{- define "dra-driver-nvidia-gpu.listHas" -}}
  {{- $listToCheck := index . 0 }}
  {{- $valueToCheck := index . 1 }}

  {{- $found := "" -}}
  {{- range $listToCheck}}
    {{- if eq . $valueToCheck }}
      {{- $found = "true" -}}
    {{- end }}
  {{- end }}
  {{- $found -}}
{{- end }}

{{/*
Filter a list by a set of valid values
*/}}
{{- define "dra-driver-nvidia-gpu.filterList" -}}
  {{- $listToFilter := index . 0 }}
  {{- $validValues := index . 1 }}

  {{- $result := list -}}
  {{- range $validValues}}
    {{- if include "dra-driver-nvidia-gpu.listHas" (list $listToFilter .) }}
      {{- $result = append $result . }}
    {{- end }}
  {{- end }}
  {{- $result -}}
{{- end -}}

{{/*
Get all namespaces (driver namespace + additional namespaces from environment variable).
After concatenation, duplicates from are removed with uniq to avoid release namespaces been
listed in ADDITIONAL_NAMESPACES, or repeated entries in the comma-separated list.
*/}}
{{- define "dra-driver-nvidia-gpu.namespaces" -}}
  {{- $driverNs := include "dra-driver-nvidia-gpu.namespace" . | trim }}
    {{- $namespaces := list $driverNs }}
    {{- if .Values.controller.containers.computeDomain.env }}
      {{- range .Values.controller.containers.computeDomain.env }}
        {{- if eq .name "ADDITIONAL_NAMESPACES" }}
          {{- if .value }}
            {{- range $raw := splitList "," .value }}
              {{- $ns := $raw | trim }}
              {{- if $ns }}
                  {{- $namespaces = concat $namespaces (list $ns) }}
              {{- end }}
            {{- end }}
          {{- end }}
      {{- end }}
    {{- end }}
  {{- end }}
  {{- join "," (uniq $namespaces) -}}
{{- end -}}

{{/*
Get the latest available resource.k8s.io API version

Priority:
  1. If .Values.resourceApiVersion is set, use that.
  2. Otherwise, returns the highest available version or empty string if none found
*/}}
{{- define "dra-driver-nvidia-gpu.resourceApiVersion" -}}
{{- if .Values.resourceApiVersion }}
{{- .Values.resourceApiVersion }}
{{- else if .Capabilities.APIVersions.Has "resource.k8s.io/v1" -}}
resource.k8s.io/v1
{{- else if .Capabilities.APIVersions.Has "resource.k8s.io/v1beta2" -}}
resource.k8s.io/v1beta2
{{- else if .Capabilities.APIVersions.Has "resource.k8s.io/v1beta1" -}}
resource.k8s.io/v1beta1
{{- else -}}
{{- end -}}
{{- end -}}

{{/*
Returns "true" when resources.computeDomains.imex.mode is exactly
"hostManaged", empty string otherwise. Used only to control structural
rendering (omitting the compute-domain-daemon DeviceClass/RBAC).
*/}}
{{- define "dra-driver-nvidia-gpu.hostManagedIMEX" -}}
{{- if eq (toString (dig "mode" "" (.Values.resources.computeDomains.imex | default dict))) "hostManaged" -}}
true
{{- end -}}
{{- end -}}

{{/*
Validates resources.computeDomains.imex.mode / .isolation against
featureGates.HostManagedIMEXDaemon so `helm install`/`helm template` fails fast
with a clear message instead of the controller/kubelet-plugin pods crash-looping
on an invalid combination. Produces no output on success.
*/}}
{{- define "dra-driver-nvidia-gpu.validateIMEXConfig" -}}
{{- $imex := .Values.resources.computeDomains.imex | default dict -}}
{{- $mode := toString (dig "mode" "driverManaged" $imex) -}}
{{- $isolation := toString (dig "isolation" "domain" $imex) -}}
{{- $gateEnabled := and .Values.featureGates (and (hasKey .Values.featureGates "HostManagedIMEXDaemon") .Values.featureGates.HostManagedIMEXDaemon) -}}
{{- if eq $mode "hostManaged" -}}
  {{- if not $gateEnabled -}}
    {{- fail "resources.computeDomains.imex.mode=hostManaged requires featureGates.HostManagedIMEXDaemon=true" -}}
  {{- end -}}
{{- else if ne $mode "driverManaged" -}}
  {{- fail (printf "unknown resources.computeDomains.imex.mode %q: must be \"driverManaged\" or \"hostManaged\"" $mode) -}}
{{- end -}}
{{- if and (ne $isolation "") (ne $isolation "domain") -}}
  {{- if eq $isolation "channel" -}}
    {{- fail "resources.computeDomains.imex.isolation=channel is not supported yet: per-workload IMEX channel allocation is not implemented. Use the default, \"domain\"." -}}
  {{- else -}}
    {{- fail (printf "unknown resources.computeDomains.imex.isolation %q: must be \"domain\" or \"channel\"" $isolation) -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Controller-owned snapshots need an expected Node set before they may become
Active. The kubelet plugin's topology label is that set in the initial
implementation.
*/}}
{{- define "dra-driver-nvidia-gpu.validateControllerOwnedCDCliques" -}}
{{- $enabled := and .Values.featureGates (and (hasKey .Values.featureGates "ControllerOwnedCDCliques") .Values.featureGates.ControllerOwnedCDCliques) -}}
{{- if and $enabled (not .Values.controllerOwnedCDCliques.admissionEnabled) -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true requires controllerOwnedCDCliques.admissionEnabled=true" -}}
{{- end -}}

{{- if and $enabled (include "dra-driver-nvidia-gpu.hostManagedIMEX" .) -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true is incompatible with resources.computeDomains.imex.mode=hostManaged" -}}
{{- end -}}
{{- if and $enabled (.Capabilities.APIVersions.Has "security.openshift.io/v1/SecurityContextConstraints") -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true is not supported on OpenShift in this alpha; SCC bindings are not yet covered by the immutable single-install admission boundary" -}}
{{- end -}}
{{- if and $enabled (ne (include "dra-driver-nvidia-gpu.resourceApiVersion" . | trim) "resource.k8s.io/v1") -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true requires the served resource.k8s.io/v1 API; Kubernetes v1.32/v1.33 beta DRA APIs remain supported only by legacy-v1" -}}
{{- end -}}
{{- if and $enabled (not .Values.kubeletPlugin.containers.computeDomains.gpuCliqueLabelEnabled) -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true requires kubeletPlugin.containers.computeDomains.gpuCliqueLabelEnabled=true" -}}
{{- end -}}
{{- if and $enabled (not .Values.controller.leaderElection.enabled) -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true requires controller.leaderElection.enabled=true; clique allocation requires one active writer" -}}
{{- end -}}
{{- if and $enabled (and (hasKey .Values.featureGates "CrashOnNVLinkFabricErrors") (not .Values.featureGates.CrashOnNVLinkFabricErrors)) -}}
  {{- fail "featureGates.ControllerOwnedCDCliques=true requires featureGates.CrashOnNVLinkFabricErrors=true; controller-owned topology must fail closed instead of falling back to non-fabric mode" -}}
{{- end -}}
{{- end -}}

{{/* Validate the installation-scoped persistent ComputeDomain agent spike. */}}
{{- define "dra-driver-nvidia-gpu.validatePersistentComputeDomainAgents" -}}
{{- $enabled := and .Values.featureGates (and (hasKey .Values.featureGates "PersistentComputeDomainAgents") .Values.featureGates.PersistentComputeDomainAgents) -}}
{{- if and $enabled (not .Values.controllerOwnedCDCliques.admissionEnabled) -}}
  {{- fail "featureGates.PersistentComputeDomainAgents=true requires controllerOwnedCDCliques.admissionEnabled=true" -}}
{{- end -}}
{{- if and $enabled (include "dra-driver-nvidia-gpu.hostManagedIMEX" .) -}}
  {{- fail "featureGates.PersistentComputeDomainAgents=true is incompatible with resources.computeDomains.imex.mode=hostManaged" -}}
{{- end -}}
{{- if and $enabled (.Capabilities.APIVersions.Has "security.openshift.io/v1/SecurityContextConstraints") -}}
  {{- fail "featureGates.PersistentComputeDomainAgents=true is not supported on OpenShift in this alpha" -}}
{{- end -}}
{{- if and $enabled (not .Values.kubeletPlugin.containers.computeDomains.gpuCliqueLabelEnabled) -}}
  {{- fail "featureGates.PersistentComputeDomainAgents=true requires kubeletPlugin.containers.computeDomains.gpuCliqueLabelEnabled=true" -}}
{{- end -}}
{{- if and $enabled (not .Values.controller.leaderElection.enabled) -}}
  {{- fail "featureGates.PersistentComputeDomainAgents=true requires controller.leaderElection.enabled=true" -}}
{{- end -}}
{{- if and $enabled (and (hasKey .Values.featureGates "CrashOnNVLinkFabricErrors") (not .Values.featureGates.CrashOnNVLinkFabricErrors)) -}}
  {{- fail "featureGates.PersistentComputeDomainAgents=true requires featureGates.CrashOnNVLinkFabricErrors=true" -}}
{{- end -}}
{{- end -}}
