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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
	pkgflags "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

type testResourceLock struct {
	createErr error
	updateErr error
}

func TestPersistentAgentInstallationNameMatchesChart(t *testing.T) {
	helpers, err := os.ReadFile(filepath.Join("..", "..", "deployments", "helm", "dra-driver-nvidia-gpu", "templates", "_helpers.tpl"))
	require.NoError(t, err)
	require.Contains(t, string(helpers), "{{- define \"dra-driver-nvidia-gpu.persistentAgentInstallationName\" -}}\n"+persistentAgentInstallationPolicyName+"\n")
}

func TestValidatePersistentAgentInstallation(t *testing.T) {
	const (
		namespace      = "control-a"
		serviceAccount = "driver-service-account-controller"
		installationID = "control-a/release-a"
	)
	valid := map[string]string{
		persistentAgentInstallationAnnotation:      installationID,
		persistentAgentControlNamespaceAnnotation:  namespace,
		persistentAgentControllerSubjectAnnotation: "system:serviceaccount:control-a:driver-service-account-controller",
	}
	tests := []struct {
		name           string
		annotations    map[string]string
		namespace      string
		serviceAccount string
		installationID string
		wantError      string
	}{
		{name: "matching installation", annotations: valid, namespace: namespace, serviceAccount: serviceAccount, installationID: installationID},
		{name: "second release", annotations: valid, namespace: namespace, serviceAccount: serviceAccount, installationID: "control-a/release-b", wantError: "Helm installation"},
		{name: "control namespace migration", annotations: valid, namespace: "control-b", serviceAccount: serviceAccount, installationID: installationID, wantError: "control namespace"},
		{name: "service account rename", annotations: valid, namespace: namespace, serviceAccount: "replacement-controller", installationID: installationID, wantError: "controller ServiceAccount"},
		{name: "missing owner annotations", annotations: map[string]string{}, namespace: namespace, serviceAccount: serviceAccount, installationID: installationID, wantError: "Helm installation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePersistentAgentInstallationIdentity(
				test.annotations,
				test.installationID,
				test.namespace,
				"system:serviceaccount:"+test.namespace+":"+test.serviceAccount,
			)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
		})
	}
}

func TestGateDisabledStartsPersistentManagersOnlyForDurableState(t *testing.T) {
	old := featuregates.Enabled(featuregates.PersistentComputeDomainAgents)
	require.NoError(t, featuregates.FeatureGates().SetFromMap(map[string]bool{string(featuregates.PersistentComputeDomainAgents): false}))
	t.Cleanup(func() {
		require.NoError(t, featuregates.FeatureGates().SetFromMap(map[string]bool{string(featuregates.PersistentComputeDomainAgents): old}))
	})
	config := &Config{clientsets: pkgflags.ClientSets{Nvidia: nvfake.NewSimpleClientset()}}
	required, err := persistentAgentStateRequired(context.Background(), config)
	require.NoError(t, err)
	require.False(t, required)

	persistentCD := &nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{
		Name: "persistent", Namespace: "workload", UID: types.UID("persistent-uid"),
		Annotations: map[string]string{nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1)},
	}}
	config.clientsets.Nvidia = nvfake.NewSimpleClientset(persistentCD)
	required, err = persistentAgentStateRequired(context.Background(), config)
	require.NoError(t, err)
	require.True(t, required)

	reservation := &nvapi.ComputeDomainCliqueReservation{ObjectMeta: metav1.ObjectMeta{Name: "clique-reservation"}}
	config.clientsets.Nvidia = nvfake.NewSimpleClientset(reservation)
	required, err = persistentAgentStateRequired(context.Background(), config)
	require.NoError(t, err)
	require.True(t, required)
}

func TestRejectLegacyComputeDomainsAllowsDeletionToFinish(t *testing.T) {
	now := metav1.Now()
	deleting := &nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{
		Name:              "deleting-legacy",
		Namespace:         "workload",
		UID:               types.UID("deleting-legacy-uid"),
		DeletionTimestamp: &now,
	}}
	config := &Config{clientsets: pkgflags.ClientSets{Nvidia: nvfake.NewSimpleClientset(deleting)}}
	require.NoError(t, rejectLegacyComputeDomains(context.Background(), config))

	active := deleting.DeepCopy()
	active.Name = "active-legacy"
	active.UID = types.UID("active-legacy-uid")
	active.DeletionTimestamp = nil
	config.clientsets.Nvidia = nvfake.NewSimpleClientset(deleting, active)
	require.ErrorContains(t, rejectLegacyComputeDomains(context.Background(), config), "active-legacy")
}

func (*testResourceLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	return nil, nil, errors.New("not implemented")
}
func (l *testResourceLock) Create(context.Context, resourcelock.LeaderElectionRecord) error {
	return l.createErr
}
func (l *testResourceLock) Update(context.Context, resourcelock.LeaderElectionRecord) error {
	return l.updateErr
}
func (*testResourceLock) RecordEvent(string) {}
func (*testResourceLock) Identity() string   { return "candidate" }
func (*testResourceLock) Describe() string   { return "test/lock" }

func TestTrackingResourceLockRecordsSuccessfulLocalAcquisition(t *testing.T) {
	base := &testResourceLock{}
	lock := &trackingResourceLock{Interface: base, identity: "candidate", acquired: make(chan struct{})}
	require.NoError(t, lock.Update(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "other"}))
	select {
	case <-lock.acquired:
		t.Fatal("another holder must not mark local acquisition")
	default:
	}
	require.NoError(t, lock.Create(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "candidate"}))
	select {
	case <-lock.acquired:
	default:
		t.Fatal("successful local acquisition was not recorded")
	}
	// Later calls are idempotent and cannot close the channel twice.
	require.NoError(t, lock.Update(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "candidate"}))
}

func TestTrackingResourceLockDoesNotRecordFailedAcquisition(t *testing.T) {
	base := &testResourceLock{updateErr: errors.New("conflict")}
	lock := &trackingResourceLock{Interface: base, identity: "candidate", acquired: make(chan struct{})}
	require.Error(t, lock.Update(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "candidate"}))
	select {
	case <-lock.acquired:
		t.Fatal("failed acquisition was recorded")
	default:
	}
}
