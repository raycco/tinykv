package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) processPut(put *raft_cmdpb.PutRequest, resp *raft_cmdpb.RaftCmdResponse) {
	err := d.peerStorage.WriteKV(put.Cf, put.Key, put.Value)
	if err != nil {
		log.Errorf("[S%d] write kv failed: %v", d.PeerId(), err)
		BindRespError(resp, err)
		return
	}

	d.SizeDiffHint += uint64(len(put.Key) + len(put.Value))

	putRsp := &raft_cmdpb.PutResponse{}
	resp.Responses = append(resp.Responses,
		&raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     putRsp,
		})
	log.Infof("[S%d] process put key = %+v value = %+v", d.PeerId(), string(put.Key), string(put.Value))
}

func (d *peerMsgHandler) processGet(get *raft_cmdpb.GetRequest, resp *raft_cmdpb.RaftCmdResponse) {
	value, err := d.peerStorage.ReadKV(get.Cf, get.Key)
	if err != nil {
		log.Errorf("[S%d] read kv failed: %v", d.PeerId(), err)
		BindRespError(resp, err)
		return
	}
	getRsp := &raft_cmdpb.GetResponse{Value: value}
	resp.Responses = append(resp.Responses,
		&raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     getRsp,
		})
	log.Infof("[S%d] process get key = %+v value = %+v", d.PeerId(), string(get.Key), string(value))
}

func (d *peerMsgHandler) processSnap(snap *raft_cmdpb.SnapRequest, resp *raft_cmdpb.RaftCmdResponse) {
	snapRsp := &raft_cmdpb.SnapResponse{Region: d.Region()}
	resp.Responses = append(resp.Responses,
		&raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    snapRsp,
		})
	log.Infof("[S%d] process snap, response = %+v", d.PeerId(), resp)
}

func (d *peerMsgHandler) processDelete(del *raft_cmdpb.DeleteRequest, resp *raft_cmdpb.RaftCmdResponse) {
	err := d.peerStorage.DeleteKV(del.Cf, del.Key)
	if err != nil {
		log.Errorf("[S%d] delete kv failed: %v", d.PeerId(), err)
		BindRespError(resp, err)
		return
	}

	d.SizeDiffHint -= uint64(len(del.Key))

	delRsp := &raft_cmdpb.DeleteResponse{}
	resp.Responses = append(resp.Responses,
		&raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  delRsp,
		})
	log.Infof("[S%d] process delete key = %+v", d.PeerId(), string(del.Key))
}

func (d *peerMsgHandler) processCompactLog(compact *raft_cmdpb.CompactLogRequest) {
	term := compact.CompactTerm
	index := compact.CompactIndex
	log.Infof("[S%d] schedule compact log, CompactIndex %+v, LastCompactedIdx %+v TruncatedIndex %+v",
		d.PeerId(), index, d.LastCompactedIdx, d.peerStorage.truncatedIndex())
	if index > d.peerStorage.truncatedIndex() {
		d.peerStorage.UpdateTruncatedState(term, index)
		d.ScheduleCompactLog(index)
	}
}

func (d *peerMsgHandler) processSplit(split *raft_cmdpb.SplitRequest) {
	if err := util.CheckKeyInRegion(split.SplitKey, d.Region()); err != nil {
		log.Warnf("[S%d] process split %+v, err: %+v, region: %+v", d.PeerId(), err, *split, d.Region())
		return
	}

	oldRegion := new(metapb.Region)
	util.CloneMsg(d.Region(), oldRegion)
	oldRegion.RegionEpoch.Version++
	newRegion := &metapb.Region{
		Id:          split.NewRegionId,
		StartKey:    split.SplitKey,
		EndKey:      oldRegion.EndKey,
		RegionEpoch: oldRegion.RegionEpoch,
	}

	var storeIds []uint64
	for _, metaPeer := range oldRegion.Peers {
		storeIds = append(storeIds, metaPeer.StoreId)
	}

	for i, peerId := range split.NewPeerIds {
		metaPeer := &metapb.Peer{
			StoreId: storeIds[i],
			Id:      peerId,
		}
		newRegion.Peers = append(newRegion.Peers, metaPeer)
	}

	oldRegion.EndKey = split.SplitKey
	d.SetRegion(oldRegion)
	d.peerStorage.UpdateRegionState(oldRegion)

	meta := d.ctx.storeMeta
	// 更新旧region的meta，否则可能checkSnapshot时版本不一致，内存和数据库一致性
	meta.Lock()
	defer meta.Unlock()
	meta.regionRanges.ReplaceOrInsert(&regionItem{oldRegion})
	meta.regions[oldRegion.Id] = oldRegion

	d.SizeDiffHint = 0

	log.Infof("[S%d] process split old %+v, new %+v", d.PeerId(), oldRegion, newRegion)

	// create peer需要加锁，因为在store_worker中收到选举请求会replicate peer，两个协程并发，
	// 有概率会使ApplyState与RaftState重复设置，导致storage的FirstIndex与LastIndex不一致
	// panic: init log entries firstIdx=1 lastIdx=5 err: requested entry at index is unavailable
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panicf("[S%d] create new peer err: %+v, new region: %+v", d.PeerId(), err, newRegion)
	}

	d.ctx.router.register(newPeer)
	meta.regionRanges.ReplaceOrInsert(&regionItem{newRegion})
	meta.regions[newRegion.Id] = newRegion
	d.peerStorage.UpdateRegionState(newRegion)
	_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

	// leader通知即可
	if d.IsLeader() {
		// 由于partition及unreliable，新创建的region可能选出leader的时间较长，
		// 而只有leader定时通知scheduler，从而导致出现find no region的错误
		notifyScheduler := func(peer *peer) {
			peer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
		notifyScheduler(d.peer)
		notifyScheduler(newPeer)
	}
}

func (d *peerMsgHandler) handleProposals(index, term uint64, resp *raft_cmdpb.RaftCmdResponse) {
	if len(d.proposals) <= 0 {
		return
	}

	idx := -1
	for i, proposal := range d.proposals {
		if index == proposal.index && term == proposal.term {
			if d.IsLeader() {
				if len(resp.Responses) > 0 && resp.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
					proposal.cb.Txn = d.peerStorage.TransactionKV()
				}
				proposal.cb.Done(resp)
				log.Infof("[S%d] process entry proposal = %+v", d.PeerId(), proposal)
			}
			idx = i
			break
		}
	}

	if idx != -1 {
		d.proposals = append(d.proposals[:idx], d.proposals[idx+1:]...)
	}
}

func (d *peerMsgHandler) processNormal(entry eraftpb.Entry) {
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	var msg raft_cmdpb.RaftCmdRequest
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		log.Infof("[S%d] %+v Unmarshal RaftCmdRequest error", d.PeerId(), err)
		BindRespError(resp, err)
	} else {
		err = util.CheckRegionEpoch(&msg, d.Region(), true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			siblingRegion := d.findSiblingRegion()
			if siblingRegion != nil {
				errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
			}
			BindRespError(resp, err)
		} else {
			for _, req := range msg.Requests {
				switch req.CmdType {
				case raft_cmdpb.CmdType_Put:
					d.processPut(req.Put, resp)
				case raft_cmdpb.CmdType_Delete:
					d.processDelete(req.Delete, resp)
				case raft_cmdpb.CmdType_Get:
					d.processGet(req.Get, resp)
				case raft_cmdpb.CmdType_Snap:
					d.processSnap(req.Snap, resp)
				}
			}
			if msg.AdminRequest != nil {
				switch msg.AdminRequest.CmdType {
				case raft_cmdpb.AdminCmdType_CompactLog:
					d.processCompactLog(msg.AdminRequest.CompactLog)
				case raft_cmdpb.AdminCmdType_Split:
					d.processSplit(msg.AdminRequest.Split)
				}
			}
		}
	}

	log.Infof("[S%d] process entry = %+v", d.PeerId(), entry)
	d.peerStorage.UpdateAppliedIndex(entry.Index)

	if len(d.proposals) <= 0 {
		return
	}

	d.handleProposals(entry.Index, entry.Term, resp)
}

func (d *peerMsgHandler) updateConf(cc eraftpb.ConfChange) bool {
	var peer metapb.Peer
	peer.Unmarshal(cc.Context)

	clonedRegion := new(metapb.Region)
	util.CloneMsg(d.Region(), clonedRegion)

	idx := -1
	for i, peer := range clonedRegion.Peers {
		if peer.Id == cc.NodeId {
			idx = i
			break
		}
	}

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
		if idx != -1 {
			return false
		} else {
			clonedRegion.Peers = append(clonedRegion.Peers, &peer)
		}
	} else {
		if idx != -1 {
			clonedRegion.Peers = append(clonedRegion.Peers[:idx], clonedRegion.Peers[idx+1:]...)
		} else {
			return false
		}
	}
	clonedRegion.RegionEpoch.ConfVer++
	d.SetRegion(clonedRegion)
	d.peerStorage.UpdateRegionState(clonedRegion)
	return true
}

func (d *peerMsgHandler) processConfChange(entry eraftpb.Entry) {
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	var cc eraftpb.ConfChange
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		log.Infof("[S%d] %+v Unmarshal ConfChange error", d.PeerId(), err)
		BindRespError(resp, err)
	} else {
		log.Infof("[S%d] apply config change = %+v", d.PeerId(), cc)
		exist := d.updateConf(cc)
		if exist {
			meta := d.ctx.storeMeta
			// defer的作用域是「函数」，不是「语句块」
			meta.Lock()
			region := d.Region()
			meta.regionRanges.ReplaceOrInsert(&regionItem{region})
			meta.regions[region.Id] = region
			meta.Unlock()

			d.RaftGroup.ApplyConfChange(cc)

			if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
				d.removePeerCache(cc.NodeId)
			}
			d.peerStorage.UpdateAppliedIndex(entry.Index)

			if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() {
				if d.MaybeDestroy() {
					d.destroyPeer()
				}
			}
		}
	}

	d.handleProposals(entry.Index, entry.Term, resp)
}

func (d *peerMsgHandler) process(entry eraftpb.Entry) {
	if d.stopped {
		return
	}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		d.processConfChange(entry)
	} else {
		d.processNormal(entry)
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		hasSnapshot := !(ready.Snapshot.Metadata == nil || ready.Snapshot.Metadata.Index == 0)
		log.Infof("[S%d] has snapshot: %+v, msgs: %+v, entries: %+v, commit entries: %+v",
			d.PeerId(), hasSnapshot, len(ready.Messages), len(ready.Entries), len(ready.CommittedEntries))
		applyResult, err := d.peerStorage.SaveReadyState(&ready)
		if err == nil && applyResult != nil {
			// conf change刚好在snap中，需要更新region
			meta := d.ctx.storeMeta
			// defer的作用域是「函数」，不是「语句块」
			meta.Lock()
			newRegion := applyResult.Region
			oldRegion := meta.regions[newRegion.Id]
			if !util.RegionEqual(oldRegion, newRegion) {
				log.Infof("[S%d] apply snapshot result: %+v", d.PeerId(), applyResult)
				meta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
				meta.regions[newRegion.Id] = newRegion
			}
			meta.Unlock()
		}
		d.Send(d.ctx.trans, ready.Messages)
		for _, entry := range ready.CommittedEntries {
			d.process(entry)
		}
		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	var key []byte
	checkKey := true
	req := msg.Requests[0]
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	default:
		checkKey = false
	}

	if checkKey {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}

	// 将整个请求序列化为字节流
	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	proposal := &proposal{d.nextProposalIndex(), d.Term(), cb}
	if err := d.RaftGroup.Propose(data); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.proposals = append(d.proposals, proposal)
}

func (d *peerMsgHandler) proposeInternal(msg *raft_cmdpb.RaftCmdRequest) {
	data, err := msg.Marshal()
	if err != nil {
		log.Warnf("[S%d] propose msg: %+v, err: %+v", d.PeerId(), *msg, err)
		return
	}

	if err := d.RaftGroup.Propose(data); err != nil {
		log.Warnf("[S%d] propose msg: %+v, err: %+v", d.PeerId(), *msg, err)
		return
	}
}

func (d *peerMsgHandler) proposeConf(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() &&
		msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		transferee := d.RaftGroup.LeadTransferee()
		d.RaftGroup.TransferLeader(transferee)
		cb.Done(ErrResp(&util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(transferee),
		}))
		return
	}
	context, _ := msg.AdminRequest.ChangePeer.Peer.Marshal()
	cc := eraftpb.ConfChange{
		ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
		NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
		Context:    context,
	}
	proposal := &proposal{d.nextProposalIndex(), d.Term(), cb}
	if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.proposals = append(d.proposals, proposal)
}

func (d *peerMsgHandler) proposeTransferLeader(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{CmdType: msg.AdminRequest.CmdType},
	}
	log.Infof("[S%d] RaftCmdResponse %+v", d.PeerId(), resp)
	cb.Done(resp)
}

func (d *peerMsgHandler) proposeSplit(msg *raft_cmdpb.RaftCmdRequest) {
	if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
		log.Warnf("[S%d] propose split msg: %+v, err: %+v", d.PeerId(), *msg, err)
		return
	}
	d.proposeInternal(msg)
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.proposeInternal(msg)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.proposeConf(msg, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.proposeSplit(msg)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.proposeTransferLeader(msg, cb)
	}
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	log.Infof("[S%d] propose raft command %+v", d.PeerId(), msg)
	// 提交至 Raft 组
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else if len(msg.Requests) > 0 {
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
