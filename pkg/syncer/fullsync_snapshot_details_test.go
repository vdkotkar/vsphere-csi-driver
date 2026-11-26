/*
Copyright 2026 The Kubernetes Authors.

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

package syncer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

type snapshotDetailsWrite struct {
	volumeID       string
	preReadVersion string
	capacityInMb   int64
}

// snapshotDetailsService lists the given CnsVolumeInfos and records snapshot detail writes.
type snapshotDetailsService struct {
	cnsvolumeinfo.VolumeInfoService
	list     []interface{}
	failFor  map[string]bool
	writes   []snapshotDetailsWrite
	managers []volumes.Manager
}

func (s *snapshotDetailsService) ListAllVolumeInfos() []interface{} { return s.list }

func (s *snapshotDetailsService) UpdateVolumeInfoSnapshotDetails(_ context.Context, volumeID,
	preReadResourceVersion string, measured *volumes.CnsSnapshotInfo, volumeManager volumes.Manager) error {
	s.managers = append(s.managers, volumeManager)
	s.writes = append(s.writes, snapshotDetailsWrite{volumeID, preReadResourceVersion,
		measured.AggregatedSnapshotCapacityInMb})
	if s.failFor[volumeID] {
		return errors.New("conflicts exhausted")
	}
	return nil
}

// fullSyncVolumeManager stands in for the CNS volume manager full sync passes along.
type fullSyncVolumeManager struct {
	volumes.Manager
	// syncedCapacityInMb is the aggregated snapshot capacity CNS reports after SyncVolume, by volume ID.
	syncedCapacityInMb map[string]int64
	syncErr            error
	synced             []string
}

func (m *fullSyncVolumeManager) SyncVolume(_ context.Context,
	syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	for _, spec := range syncVolumeSpecs {
		m.synced = append(m.synced, spec.VolumeId.Id)
	}
	return "", m.syncErr
}

func (m *fullSyncVolumeManager) QueryVolume(_ context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	result := &cnstypes.CnsQueryResult{}
	for _, volumeID := range queryFilter.VolumeIds {
		if capacityInMb, ok := m.syncedCapacityInMb[volumeID.Id]; ok {
			result.Volumes = append(result.Volumes, cnsBlockVolume(volumeID.Id, capacityInMb))
		}
	}
	return result, nil
}

func useSnapshotDetailsService(t *testing.T, s *snapshotDetailsService) {
	t.Helper()
	saved := volumeInfoService
	volumeInfoService = s
	t.Cleanup(func() { volumeInfoService = saved })
}

// preReadCVI returns a CnsVolumeInfo read before the CNS query. capacityInMb < 0 means no valid size.
func preReadCVI(volumeID, resourceVersion string, capacityInMb int64) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	cvi := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{Name: volumeID, ResourceVersion: resourceVersion},
		Spec:       cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{VolumeID: volumeID},
	}
	if capacityInMb >= 0 {
		cvi.Spec.ValidAggregatedSnapshotSize = true
		cvi.Spec.AggregatedSnapshotSize = resource.NewQuantity(capacityInMb*common.MbInBytes, resource.BinarySI)
	}
	return cvi
}

func cnsBlockVolume(volumeID string, capacityInMb int64) cnstypes.CnsVolume {
	return cnstypes.CnsVolume{
		VolumeId:             cnstypes.CnsVolumeId{Id: volumeID},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{AggregatedSnapshotCapacityInMb: capacityInMb},
	}
}

func TestListVolumeInfosByVolumeID(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(preReadCVI("vol-1", "17", 5))
	require.NoError(t, err)
	useSnapshotDetailsService(t, &snapshotDetailsService{list: []interface{}{
		&unstructured.Unstructured{Object: obj},
		"not a CnsVolumeInfo",
	}})

	got := listVolumeInfosByVolumeID(ctx)

	require.Len(t, got, 1)
	require.Contains(t, got, "vol-1")
	assert.Equal(t, "17", got["vol-1"].ResourceVersion)
	assert.Equal(t, int64(5*common.MbInBytes), got["vol-1"].Spec.AggregatedSnapshotSize.Value())
}

func TestValidateAndCorrectVolumeInfoSnapshotDetails(t *testing.T) {
	tests := []struct {
		name               string
		preRead            *cnsvolumeinfov1alpha1.CNSVolumeInfo
		cnsVolume          *cnstypes.CnsVolume
		syncedCapacityInMb map[string]int64
		syncErr            error
		wantWrites         []snapshotDetailsWrite
		wantSynced         []string
	}{
		{
			name:      "in sync with CNS",
			preRead:   preReadCVI("vol-1", "3", 5),
			cnsVolume: ptrTo(cnsBlockVolume("vol-1", 5)),
		},
		{
			name:               "mismatch is corrected with the pre-read version",
			preRead:            preReadCVI("vol-1", "3", 5),
			cnsVolume:          ptrTo(cnsBlockVolume("vol-1", 8)),
			syncedCapacityInMb: map[string]int64{"vol-1": 8},
			wantWrites:         []snapshotDetailsWrite{{"vol-1", "3", 8}},
			wantSynced:         []string{"vol-1"},
		},
		{
			name:               "mismatch is corrected with the size CNS reports after SyncVolume",
			preRead:            preReadCVI("vol-1", "3", 5),
			cnsVolume:          ptrTo(cnsBlockVolume("vol-1", 0)),
			syncedCapacityInMb: map[string]int64{"vol-1": 8},
			wantWrites:         []snapshotDetailsWrite{{"vol-1", "3", 8}},
			wantSynced:         []string{"vol-1"},
		},
		{
			name:               "size not yet refreshed in the CNS query is not overwritten",
			preRead:            preReadCVI("vol-1", "3", 1),
			cnsVolume:          ptrTo(cnsBlockVolume("vol-1", 0)),
			syncedCapacityInMb: map[string]int64{"vol-1": 1},
			wantSynced:         []string{"vol-1"},
		},
		{
			name:       "SyncVolume failure falls back to the full sync query result",
			preRead:    preReadCVI("vol-1", "3", 5),
			cnsVolume:  ptrTo(cnsBlockVolume("vol-1", 8)),
			syncErr:    errors.New("sync failed"),
			wantWrites: []snapshotDetailsWrite{{"vol-1", "3", 8}},
			wantSynced: []string{"vol-1"},
		},
		{
			name:       "query failure after SyncVolume falls back to the full sync query result",
			preRead:    preReadCVI("vol-1", "3", 5),
			cnsVolume:  ptrTo(cnsBlockVolume("vol-1", 8)),
			wantWrites: []snapshotDetailsWrite{{"vol-1", "3", 8}},
			wantSynced: []string{"vol-1"},
		},
		{
			name:       "missing size is filled in",
			preRead:    preReadCVI("vol-1", "3", -1),
			cnsVolume:  ptrTo(cnsBlockVolume("vol-1", 8)),
			wantWrites: []snapshotDetailsWrite{{"vol-1", "3", 8}},
		},
		{
			name:               "size CNS can no longer report is marked invalid",
			preRead:            preReadCVI("vol-1", "3", 5),
			cnsVolume:          ptrTo(cnsBlockVolume("vol-1", -1)),
			syncedCapacityInMb: map[string]int64{"vol-1": -1},
			wantWrites:         []snapshotDetailsWrite{{"vol-1", "3", -1}},
			wantSynced:         []string{"vol-1"},
		},
		{
			name:      "invalid in both CNS and CnsVolumeInfo",
			preRead:   preReadCVI("vol-1", "3", -1),
			cnsVolume: ptrTo(cnsBlockVolume("vol-1", -1)),
		},
		{
			name:    "volume not returned by CNS",
			preRead: preReadCVI("vol-1", "3", 5),
		},
		{
			name:    "not a block volume",
			preRead: preReadCVI("vol-1", "3", 5),
			cnsVolume: &cnstypes.CnsVolume{
				VolumeId:             cnstypes.CnsVolumeId{Id: "vol-1"},
				BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logger.NewContextWithLogger(context.Background())
			service := &snapshotDetailsService{}
			useSnapshotDetailsService(t, service)
			cnsVolumes := map[string]cnstypes.CnsVolume{}
			if tc.cnsVolume != nil {
				cnsVolumes[tc.cnsVolume.VolumeId.Id] = *tc.cnsVolume
			}

			volManager := &fullSyncVolumeManager{syncedCapacityInMb: tc.syncedCapacityInMb, syncErr: tc.syncErr}

			validateAndCorrectVolumeInfoSnapshotDetails(ctx, volManager, cnsVolumes,
				map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo{tc.preRead.Spec.VolumeID: tc.preRead})

			assert.Equal(t, tc.wantWrites, service.writes)
			assert.Equal(t, tc.wantSynced, volManager.synced)
			for _, m := range service.managers {
				assert.Same(t, volManager, m, "CNS is re-measured through the full sync volume manager")
			}
		})
	}
}

// A failure for one volume does not stop the others.
func TestValidateAndCorrectVolumeInfoSnapshotDetailsContinuesAfterFailure(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	service := &snapshotDetailsService{failFor: map[string]bool{"vol-1": true}}
	useSnapshotDetailsService(t, service)

	validateAndCorrectVolumeInfoSnapshotDetails(ctx,
		&fullSyncVolumeManager{syncedCapacityInMb: map[string]int64{"vol-1": 8, "vol-2": 9}},
		map[string]cnstypes.CnsVolume{
			"vol-1": cnsBlockVolume("vol-1", 8),
			"vol-2": cnsBlockVolume("vol-2", 9),
		},
		map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo{
			"vol-1": preReadCVI("vol-1", "3", 5),
			"vol-2": preReadCVI("vol-2", "4", 5),
		})

	assert.ElementsMatch(t, []snapshotDetailsWrite{
		{"vol-1", "3", 8},
		{"vol-2", "4", 9},
	}, service.writes)
}
