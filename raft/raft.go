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
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	random              RandomGenerator
	randElectionTimeout int

	logPrefix string
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		log.Panicf("init raft initial state err: %+v", err)
	}
	raft := &Raft{
		id:                  c.ID,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		RaftLog:             newLog(c.Storage),
		votes:               make(map[uint64]bool),
		Prs:                 make(map[uint64]*Progress),
		State:               StateFollower,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		randElectionTimeout: c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
	}

	if len(confState.Nodes) > 0 {
		for _, peer := range confState.Nodes {
			raft.Prs[peer] = &Progress{hardState.Commit, hardState.Commit + 1}
		}
	} else {
		for _, peer := range c.peers {
			raft.Prs[peer] = &Progress{0, 1}
		}
	}

	raft.RaftLog.applied = c.Applied
	firstIdx, err := raft.RaftLog.FirstIndex()
	if err != nil {
		log.Panicf("init raft first index err: %+v", err)
	}
	if raft.RaftLog.applied < firstIdx {
		raft.RaftLog.applied = firstIdx - 1
	}

	raft.logPrefix = fmt.Sprintf("[S%d]", raft.id)

	raft.random = *NewRandomGenerator(c.ElectionTick)
	raft.resetElectionTimeout()

	return raft
}

func (r *Raft) sendAppends() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	r.resetElectionTimeout()
}

func (r *Raft) buildAppend(prevLogIndex uint64, to uint64) pb.Message {
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		log.Panicf("send append entries prev log term err: %+v", err)
	}

	log.Infof("%s send append entries to [S%d]: prevLogIndex=%+v, prevLogTerm=%+v, stabled=%+v",
		r.logPrefix, to, prevLogIndex, prevLogTerm, r.RaftLog.stabled)

	//entries := make([]pb.Entry, r.RaftLog.LastIndex()-prevLogIndex)
	lastIdx := r.RaftLog.LastIndex()
	var entries []pb.Entry
	for _, entry := range r.RaftLog.allEntries() {
		if entry.Index >= r.Prs[to].Next {
			entries = append(entries, entry)
		}
	}
	//copy(entries, r.RaftLog.allEntries()[lastIdx-prevLogIndex:])

	log.Debugf("%s send append entries to [S%d]: lastIdx=%+v, push entries = %+v", r.logPrefix, to, lastIdx, entries)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Commit:  r.RaftLog.committed,
	}

	for _, entry := range entries {
		entryCopy := entry
		msg.Entries = append(msg.Entries, &entryCopy)
	}

	log.Infof("%s send append entries to [S%d]: lastIdx=%+v, push msg = %+v", r.logPrefix, to, lastIdx, msg)

	if len(msg.Entries) > 0 {
		matchIndex := msg.Entries[len(msg.Entries)-1].Index
		r.Prs[r.id] = &Progress{matchIndex, matchIndex + 1}
		log.Infof("%s send append entries to [S%d]: progress = %+v ", r.logPrefix, to, r.Prs[r.id])
	}
	return msg
}

func (r *Raft) buildSnapshotMsg(prevLogIndex uint64, to uint64, snapshot *pb.Snapshot) pb.Message {
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		LogTerm:  prevLogTerm,
		Index:    prevLogIndex,
		Commit:   r.RaftLog.committed,
		Snapshot: snapshot,
	}

	r.Prs[to] = &Progress{snapshot.Metadata.Index, snapshot.Metadata.Index + 1}

	log.Infof("%s send snapshot to [S%d]: lastIdx=%+v, push msg = %+v", r.logPrefix, to, r.RaftLog.LastIndex(), msg)
	return msg
}

func (r *Raft) buildSnapshot(prevLogIndex uint64, to uint64) pb.Message {
	var msg pb.Message
	if r.RaftLog.pendingSnapshot != nil {
		return r.buildSnapshotMsg(prevLogIndex, to, r.RaftLog.pendingSnapshot)
	}
	for {
		snapshot, err := r.RaftLog.storage.Snapshot()
		log.Infof("%s request snapshot for [S%d]: snapshot = %+v err: %+v", r.logPrefix, to, snapshot, err)
		if err == ErrSnapshotTemporarilyUnavailable {
			time.Sleep(20 * time.Millisecond)
			continue
		} else {
			if err == nil {
				msg = r.buildSnapshotMsg(prevLogIndex, to, &snapshot)
			} else {
				msg = r.buildAppend(prevLogIndex, to)
			}
			break
		}
	}

	return msg
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var msg pb.Message
	prevLogIndex := r.Prs[to].Next - 1
	firstIdx, err := r.RaftLog.FirstIndex()
	if err != nil {
		log.Panicf("send append entries to %v err: %v", to, err)
	}
	log.Infof("%s send append entries to [S%d]: nextIdx=%+v, firstIdx=%+v, applied=%+v",
		r.logPrefix, to, r.Prs[to].Next, firstIdx, r.RaftLog.applied)
	if r.Prs[to].Next < firstIdx || r.RaftLog.pendingSnapshot != nil {
		msg = r.buildSnapshot(prevLogIndex, to)
	} else {
		msg = r.buildAppend(prevLogIndex, to)
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	log.Infof("%s send heartbeat to [S%d]: matchIdx=%+v, leader matchIdx=%+v", r.logPrefix, to, r.Prs[to], r.Prs[r.id])
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends a request vote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	lastIdx := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(lastIdx)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   lastIdx,
		LogTerm: term,
	}

	r.msgs = append(r.msgs, msg)
	log.Infof("%s send request vote to [S%d]: msg = %+v", r.logPrefix, to, msg)
}

func (r *Raft) resetElectionTimeout() {
	r.randElectionTimeout = r.electionTimeout + r.random.Next()
	r.electionElapsed = 0
}

func (r *Raft) startElection() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	for peer, prs := range r.Prs {
		log.Debugf("%s start election: peer=%+v, progress=%+v", r.logPrefix, peer, prs)
	}

	r.votes = make(map[uint64]bool)
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
	r.resetElectionTimeout()
}

func (r *Raft) sendHeartbeats() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
	r.heartbeatElapsed = 0
	r.resetElectionTimeout()
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	if r.electionElapsed >= r.randElectionTimeout {
		log.Infof("%s election elapsed: %+v", r.logPrefix, r.electionElapsed)
		r.startElection()
	}

	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.sendHeartbeats()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = 0
	r.Lead = lead
	r.leadTransferee = 0
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Vote = 0
	r.Lead = r.id
	r.votes = make(map[uint64]bool)

	// sync little log when leader change
	for peer := range r.Prs {
		r.Prs[peer] = &Progress{r.RaftLog.committed, r.RaftLog.LastIndex() + 1}
	}

	r.startNoop()

	log.Infof("%s become leader: lastIdx=%+v", r.logPrefix, r.RaftLog.LastIndex())
	r.sendAppends()
}

func (r *Raft) stepLeader(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgBeat:
		r.sendHeartbeats()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteRsp(m)
	case pb.MessageType_MsgAppend:
		if r.Term == m.Term {
			r.becomeFollower(m.Term, r.Lead)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesRsp(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatRsp(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteRsp(m)
	case pb.MessageType_MsgAppend:
		if r.Term == m.Term {
			r.becomeFollower(m.Term, r.Lead)
		}
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		if r.Term == m.Term {
			r.becomeFollower(m.Term, r.Lead)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.startElection()
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Infof("%s step message: term=%v, state=%+v, message = %+v", r.logPrefix, r.Term, r.State, m)

	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) startNoop() {
	entries := []*pb.Entry{{}}
	r.startOps(entries)
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

func (r *Raft) startOps(entries []*pb.Entry) {
	for _, pEntry := range entries {
		if pEntry != nil {
			entry := pb.Entry{
				EntryType: pEntry.EntryType,
				Index:     r.RaftLog.LastIndex() + 1,
				Term:      r.Term,
				Data:      pEntry.Data,
			}
			r.RaftLog.entries = append(r.RaftLog.entries, entry)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != 0 {
		return
	}
	r.startOps(m.Entries)
	log.Infof("%s handle propose: lastIdx=%+v", r.logPrefix, r.RaftLog.LastIndex())
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	r.sendAppends()
}

func (r *Raft) searchConflictIndex(prevLogIndex uint64) uint64 {
	index := prevLogIndex - 1
	termPreIdx, _ := r.RaftLog.Term(prevLogIndex)
	for {
		termIdx, _ := r.RaftLog.Term(index)
		if termPreIdx != termIdx {
			log.Infof("%s search conflict Index=%+v, Term=%+v, prevIndex=%+v, prevTerm=%+v",
				r.logPrefix, index, termIdx, prevLogIndex, termPreIdx)
			break
		}
		index--
	}
	return index + 1
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Debugf("%s handle append entries: msg = %+v", r.logPrefix, m)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	r.resetElectionTimeout()

	// 1. Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	r.becomeFollower(m.Term, m.From)

	firstIdx, err := r.RaftLog.FirstIndex()
	if err != nil {
		log.Panicf("handle append entries first index err: %+v", err)
	}

	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		log.Debugf("handle append entries prev term %+v err: %+v", term, err.Error())
	}

	lastIdx := r.RaftLog.LastIndex()
	log.Infof("%s handle append entries: prevIndex=%+v, prevLogTerm=%+v, firstIdx=%+v, lastIdx=%+v term=%+v",
		r.logPrefix, m.Index, m.LogTerm, firstIdx, lastIdx, term)

	if firstIdx > 0 && m.Index < firstIdx-1 {
		// m.Index: 291 m.LogTerm: 18, lastindex: 319 term 0
		msg.Index = firstIdx
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		log.Infof("%s handle append entries: conflict index=%+v because compact", r.logPrefix, msg.Index)
		return
	}

	if lastIdx < m.Index || term != m.LogTerm {
		msg.Reject = true
		if lastIdx < m.Index || m.Index == 0 {
			msg.Index = lastIdx + 1
		} else {
			msg.Index = r.searchConflictIndex(m.Index)
		}
		log.Infof("%s handle append entries: conflict index=%+v", r.logPrefix, msg.Index)
		r.msgs = append(r.msgs, msg)
		return
	}

	index := m.Index + 1
	conflict := false
	for i := 0; i < len(m.Entries) && index <= lastIdx; i++ {
		term, _ := r.RaftLog.Term(index)
		log.Infof("%s handle append entries: i=%d, index=%+v term=%+v, entry term=%+v", r.logPrefix, i, index, term, m.Entries[i].Term)
		if term != m.Entries[i].Term && i < len(m.Entries) {
			conflict = true
			break
		}
		index += 1
	}

	// Append any new entries not already in the log
	var entries []pb.Entry
	for _, entry := range m.Entries {
		if entry.Index >= index {
			entryCopy := *entry
			entries = append(entries, entryCopy)
		}
	}

	log.Infof("%s handle append entries: stabled=%+v, index=%+v, new entries = %+v",
		r.logPrefix, r.RaftLog.stabled, index, entries)

	if len(entries) > 0 {
		r.RaftLog.appendEnts(entries, index, conflict)
	}

	log.Infof("%s handle append entries: stabled=%+v, commit=%+v, memory entries = %+v",
		r.logPrefix, r.RaftLog.stabled, r.RaftLog.committed, r.RaftLog.entries)

	if len(entries) > 0 {
		msg.Index = entries[len(entries)-1].Index
	} else {
		// follower的日志已经和leader同步，由于回复丢失，导致一直transferleader
		msg.Index = r.RaftLog.LastIndex()
	}
	msg.Reject = false
	r.msgs = append(r.msgs, msg)

	var newLastIdx uint64
	if len(entries) == 0 {
		newLastIdx = index - 1
	} else {
		newLastIdx = r.RaftLog.LastIndex()
	}
	r.Prs[r.id] = &Progress{newLastIdx, newLastIdx + 1}

	if m.Commit > r.RaftLog.committed {
		if m.Commit <= newLastIdx {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = newLastIdx
		}
	}

	log.Infof("%s handle append entries: stabled=%+v, commit=%+v, lastIdx=%+v",
		r.logPrefix, r.RaftLog.stabled, r.RaftLog.committed, newLastIdx)
}

func (r *Raft) updateMatchCommit(from uint64) {
	count := 1
	index := r.Prs[from].Match
	for peer, prs := range r.Prs {
		if peer != r.id && prs.Match > r.RaftLog.committed {
			count++
			if prs.Match < index { // the min index for commit
				index = prs.Match
			}
		}
	}

	term, err := r.RaftLog.Term(index)
	if err != nil {
		log.Debugf("%s update match: index=%d, err = %+v ", r.logPrefix, index, err)
	}

	if count > len(r.Prs)/2 && term == r.Term {
		r.RaftLog.committed = index
		log.Infof("%s update match: commit=%d, stabled=%+v ", r.logPrefix, r.RaftLog.committed, r.RaftLog.stabled)
		r.sendAppends()
	}
}

func (r *Raft) handleAppendEntriesRsp(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	if m.Reject {
		if m.Term == r.Term && m.Index < r.Prs[m.From].Next {
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From)
		}
	} else {
		if m.Term == r.Term {
			if m.Index > r.Prs[m.From].Match {
				r.Prs[m.From] = &Progress{m.Index, m.Index + 1}
			}

			log.Infof("%s handle append response from %+v: progress=%+v, commit=%d",
				r.logPrefix, m.From, r.Prs[m.From], r.RaftLog.committed)

			r.updateMatchCommit(m.From)
			if r.leadTransferee != 0 && r.Prs[r.leadTransferee].Next > r.RaftLog.LastIndex() {
				r.sendTimeoutNow(r.leadTransferee)
			}

		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%s handle heartbeat: commit=%+v, match=%+v ", r.logPrefix, r.RaftLog.committed, r.Prs[r.id])
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Commit:  r.RaftLog.committed,
	}

	r.resetElectionTimeout()
	r.msgs = append(r.msgs, msg)
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	log.Infof("%s handle request vote from [S%d]: Term=%+v Vote=%+v, m.Term=%+v",
		r.logPrefix, m.From, r.Term, r.Vote, m.Term)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	}

	if m.Term < r.Term || (r.Vote != None && m.From != r.Vote) {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	log.Infof("%s handle request vote from [S%d]: lastIndex=%+v lastLogTerm=%+v ",
		r.logPrefix, m.From, lastIndex, lastLogTerm)
	log.Infof("%s handle request vote from [S%d]: m.LogTerm=%+v m.Index=%+v ",
		r.logPrefix, m.From, m.LogTerm, m.Index)
	if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < lastIndex) {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	msg.Reject = false
	r.Vote = m.From
	r.msgs = append(r.msgs, msg)
	r.resetElectionTimeout()
}

// handleRequestVoteRsp handle RequestVote RPC response
func (r *Raft) handleRequestVoteRsp(m pb.Message) {
	r.votes[m.From] = !m.Reject

	count := 0
	for _, reject := range r.votes {
		if reject {
			count++
		}
	}

	log.Infof("%s handle request vote rsp from [S%d]: votes=%+v, peer count=%+v",
		r.logPrefix, m.From, r.votes, len(r.Prs))
	if count > len(r.Prs)/2 {
		r.becomeLeader()
		//r.sendHeartbeats()
	}

	if len(r.votes) == len(r.Prs) && count <= len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// handleHeartBeatRsp handle HeartBeat RPC response
func (r *Raft) handleHeartbeatRsp(m pb.Message) {
	if m.Index < r.RaftLog.LastIndex() || m.Commit < r.RaftLog.committed {
		if m.Commit < r.Prs[m.From].Match {
			r.Prs[m.From] = &Progress{m.Commit, m.Commit + 1}
		}
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}

	if r.Term > m.Term {
		m.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	r.becomeFollower(m.Term, m.From)

	hasPendingSnapshot := !IsEmptySnap(r.RaftLog.pendingSnapshot)
	log.Infof("%s handle snapshot from [S%d]: pending=%+v, commit=%+v",
		r.logPrefix, m.From, hasPendingSnapshot, r.RaftLog.committed)
	if hasPendingSnapshot ||
		m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		msg.Index = r.RaftLog.committed
		msg.Reject = false
		r.msgs = append(r.msgs, msg)
		return
	}

	r.RaftLog.pendingSnapshot = m.GetSnapshot()
	log.Infof("%s handle snapshot from [S%d]: %+v", r.logPrefix, m.From, *r.RaftLog.pendingSnapshot)
	r.RaftLog.committed = r.RaftLog.pendingSnapshot.Metadata.Index
	r.RaftLog.applied = r.RaftLog.committed
	r.RaftLog.stabled = r.RaftLog.committed

	if len(m.Snapshot.Metadata.ConfState.Nodes) > 0 {
		r.Prs = make(map[uint64]*Progress)
		for _, peer := range m.Snapshot.Metadata.ConfState.Nodes {
			r.Prs[peer] = &Progress{r.RaftLog.committed, r.RaftLog.committed + 1}
		}
	}
	msg.Index = r.RaftLog.committed
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{0, 1}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		return
	}
	delete(r.Prs, id)
	if len(r.Prs) <= 0 {
		return
	}

	if r.State == StateLeader && id != r.id {
		r.updateMatchCommit(r.id)
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if r.State == StateLeader {
		if m.From <= 0 || r.id == m.From {
			return
		}

		if _, ok := r.Prs[m.From]; !ok {
			return
		}

		r.leadTransferee = m.From
		r.sendTimeoutNow(m.From)
	} else {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	log.Infof("%s send timeout now to [S%d]: nextIdx=%+v, lastIdx=%+v, matchIdx=%+v commit=%+v",
		r.logPrefix, to, r.Prs[to].Next, r.RaftLog.LastIndex(), r.Prs[to].Match, r.RaftLog.committed)
	if r.Prs[to].Next < r.RaftLog.LastIndex() ||
		r.Prs[to].Match < r.RaftLog.committed {
		r.sendAppend(to)
		return
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) LeadTransferee() uint64 {
	id := r.id
	var match uint64
	for peer, prs := range r.Prs {
		if peer != r.id && prs.Match > match {
			id = peer
			match = prs.Match
		}
	}
	return id
}
