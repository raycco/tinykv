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
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	stores := cluster.GetStores()
	upCount := 0
	for i, store := range stores {
		log.Debug("store", zap.Int("i", i), zap.Uint64("id", store.GetID()), zap.Bool("up", store.IsUp()),
			zap.Duration("downtime", store.DownTime()), zap.Int64("region size", store.GetRegionSize()))
		if store.IsUp() {
			upCount++
		}
	}
	if upCount <= cluster.GetMaxReplicas() {
		return nil
	}

	var region *core.RegionInfo
	// 优先级队列：pending -> follower -> leader
	handles := []func(storeID uint64, callback func(core.RegionsContainer)){
		cluster.GetPendingRegionsWithLock,
		cluster.GetFollowersWithLock,
		cluster.GetLeadersWithLock,
	}

	for _, h := range handles {
		region = s.GetRandomRegion(stores, h)
		if region != nil {
			break
		}
	}

	if region == nil {
		return nil
	}

	log.Debug("region", zap.Uint64("id", region.GetID()),
		zap.Int64("approximate", region.GetApproximateSize()), zap.Stringer("meta", region.GetMeta()))

	storeIds := region.GetStoreIds()
	sources := make([]*core.StoreInfo, 0)
	targets := make([]*core.StoreInfo, 0)
	for _, store := range stores {
		if _, ok := storeIds[store.GetID()]; ok {
			sources = append(sources, store)
		} else {
			targets = append(targets, store)
		}
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})

	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	log.Debug("store", zap.Duration("maxStoreDownTime", maxStoreDownTime))

	var source *core.StoreInfo
	for _, store := range sources {
		if store.IsUp() && store.DownTime() < maxStoreDownTime {
			source = store
			break
		}
	}

	var target *core.StoreInfo
	for _, store := range targets {
		if store.IsUp() && store.DownTime() < maxStoreDownTime {
			target = store
			break
		}
	}

	if source == nil || target == nil {
		return nil
	}

	log.Info("result", zap.Uint64("source store id", source.GetID()), zap.Uint64("target store id", target.GetID()))

	return s.CreateMovePeerOperator(cluster, region, source, target)
}

func (s *balanceRegionScheduler) GetRandomRegion(stores []*core.StoreInfo,
	GetRegionsWithLock func(storeID uint64, callback func(core.RegionsContainer))) *core.RegionInfo {
	var region *core.RegionInfo

	for _, store := range stores {
		GetRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			break
		}
	}

	return region
}

func (s *balanceRegionScheduler) CreateMovePeerOperator(
	cluster opt.Cluster, region *core.RegionInfo, source, target *core.StoreInfo) *operator.Operator {
	peer := region.GetStorePeer(source.GetID())
	if source.GetRegionSize()-target.GetRegionSize() > 2*region.GetApproximateSize() {
		for i := 0; i < balanceRegionRetryLimit; i++ {
			op, err := operator.CreateMovePeerOperator(
				s.GetType(), cluster, region, operator.OpBalance, source.GetID(), target.GetID(), peer.GetId())
			if err == nil {
				return op
			} else {
				log.Info("opertator", zap.String("err", err.Error()))
			}
		}
	}

	return nil
}
