// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// select all suitable stores
	suitStores := make([]*core.StoreInfo, 0)
	for _ , store := range cluster.GetStores() {
		// should be up and the downTime cannot be longer than MaxStoreDownTime of the cluster
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime(){
			suitStores = append(suitStores, store)
		}
	}

	if len(suitStores) == 1 || len(suitStores) == 0 {
		return nil
	}

	// sort them according to their region size
	// 递减排序
	sort.Slice(suitStores, func(i,j int) bool{
		return suitStores[i].GetRegionSize() > suitStores[j].GetRegionSize()
	})

	// First, it will try to select a pending region
	var region *core.RegionInfo
	for _, suitStore := range suitStores {
		cluster.GetPendingRegionsWithLock(suitStore.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil,nil)
		})
		if region != nil {
			break
		}
		cluster.GetFollowersWithLock(suitStore.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil,nil)
		})
		if region != nil {
			break
		}
		cluster.GetLeadersWithLock(suitStore.GetID(), func(container core.RegionsContainer) {
			region = container.RandomRegion(nil,nil)
		})
		if region != nil {
			break
		}
	}

	if region == nil {
		return nil
	}
	if len(region.GetStoreIds()) < cluster.GetMaxReplicas(){
		return nil
	}

	var target *core.StoreInfo
	var source *core.StoreInfo
	source = suitStores[0]

	for i := len(suitStores)-1; i >= 0; i-- {
		suitStore := suitStores[i]
		exist := region.GetStorePeer(suitStore.GetID())
		if exist == nil {
			target = suitStore
			break
		}
	}

	if target == nil {
		return nil
	}

	// make sure that the difference has to be bigger than two times the approximate size of the region
	diff := source.GetRegionSize() - target.GetRegionSize()
	if diff <= 2* region.GetApproximateSize() {
		return nil
	}

	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		panic(err)
	}
	op, err := operator.CreateMovePeerOperator("balance_region", cluster,region, operator.OpBalance, source.GetID(),target.GetID(),newPeer.GetId())
	if err != nil {
		panic(err)
	}
	return op
}
