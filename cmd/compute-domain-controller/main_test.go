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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type testResourceLock struct {
	createErr error
	updateErr error
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
