// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).

	logPrefix string
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	rawNode := &RawNode{Raft: newRaft(config)}
	rawNode.logPrefix = fmt.Sprintf("[S%d]", rawNode.Raft.id)
	return rawNode, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	log.Infof("%s ready: applied = %+v, committed = %+v, lastIndex = %+v",
		rn.logPrefix, rn.Raft.RaftLog.applied, rn.Raft.RaftLog.committed, rn.Raft.RaftLog.LastIndex())
	entries := rn.Raft.RaftLog.unstableEntries()
	commitedEnts := rn.Raft.RaftLog.nextEnts()
	msgs := rn.Raft.msgs

	var hardState pb.HardState
	if len(rn.Raft.msgs) > 0 || !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		hardState = pb.HardState{Term: rn.Raft.Term, Vote: rn.Raft.Vote, Commit: rn.Raft.RaftLog.committed}
	} else {
		hardState = pb.HardState{}
	}

	var snapshot pb.Snapshot
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	var softState *SoftState
	if rn.Raft.Lead != 0 {
		softState = &SoftState{rn.Raft.Lead, rn.Raft.State}
	}

	return Ready{softState, hardState, entries, snapshot, commitedEnts, msgs}
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	if len(rn.Raft.msgs) > 0 ||
		rn.Raft.RaftLog.applied < rn.Raft.RaftLog.committed ||
		!IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).

	// 当前storage日志219到228，applied = 223，收到snap = 224，收到append
	// 的commit = 228，append index 229与230，memory = 230，日志变成219到230
	// 先应用snap，storage = 224，清空storage中的日志，然后process，最后advance
	// applied = 224, commit = 228, snap = 224, storage = 224, memory = 230
	// 注：此时应用snap只应该删除225之前的日志，而非删除当前storage中所有日志
	var applied, stabled uint64
	firstIdx, _ := rn.Raft.RaftLog.storage.FirstIndex()
	hasSnapshot := !IsEmptySnap(&rd.Snapshot)
	if hasSnapshot && firstIdx > 0 && rd.Snapshot.Metadata.Index >= firstIdx-1 {
		// 处理snapshot meta index:243，apply snapshot时truncated_state从index:216变为243，
		// 此时first index = truncated index + 1，因此需要加上等号才能重置pendingSnapshot
		applied = rd.Snapshot.Metadata.Index
		stabled = rd.Snapshot.Metadata.Index
		rn.Raft.RaftLog.pendingSnapshot = nil
	} else if len(rd.Entries) > 0 && !reflect.DeepEqual(rd.Entries, []eraftpb.Entry{}) {
		stabled = rd.Entries[len(rd.Entries)-1].Index
	}

	if len(rd.CommittedEntries) > 0 {
		applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}

	if stabled < applied {
		stabled = applied
	}

	rn.Raft.RaftLog.AdvanceApplied(applied)
	rn.Raft.RaftLog.AdvanceStabled(stabled)

	rn.Raft.msgs = make([]pb.Message, 0)
	//log.Debugf("%s advance: all entries %+v", rn.logPrefix, rn.Raft.RaftLog.allEntries())
	log.Infof("%s advance: snapshot = %+v, firstIdx = %+v, applied = %+v, stabled = %+v",
		rn.logPrefix, hasSnapshot, firstIdx, rn.Raft.RaftLog.applied, rn.Raft.RaftLog.stabled)
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

func (rn *RawNode) LeadTransferee() uint64 {
	return rn.Raft.LeadTransferee()
}
