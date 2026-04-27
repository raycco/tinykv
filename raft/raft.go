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

	hardState, _, _ := c.Storage.InitialState()
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
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{0, 1}
	}

	raft.logPrefix = fmt.Sprintf("[S%d]", raft.id)

	raft.random = *NewRandomGenerator(c.ElectionTick)
	log.New()

	return raft
}

func (r *Raft) sendAppends() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	log.Infof("%s send append prevLogIndex: %+v, prevLogTerm: %+v, stable: %+v",
		r.logPrefix, prevLogIndex, prevLogTerm, r.RaftLog.stabled)
	log.Infof("%s send append entries: %+v", r.logPrefix, r.RaftLog.entries)
	entries := make([]pb.Entry, r.RaftLog.LastIndex()-prevLogIndex)
	copy(entries, r.RaftLog.allEntries()[prevLogIndex:])

	log.Infof("%s send append LII: %+v, push entries: %+v", r.logPrefix, r.RaftLog.LastIndex(), entries)
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

	log.Infof("%s send append LII: %+v, push msg: %+v", r.logPrefix, r.RaftLog.LastIndex(), msg)

	if len(msg.Entries) > 0 {
		matchIndex := msg.Entries[len(msg.Entries)-1].Index
		r.Prs[r.id] = &Progress{matchIndex, matchIndex + 1}
		log.Infof("%s send append progress: %+v ", r.logPrefix, r.Prs[r.id])
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	log.Infof("%s send heartbeat to %+v match %+v leader match %+v", r.logPrefix, to, r.Prs[to], r.Prs[r.id])
	term, _ := r.RaftLog.Term(r.Prs[r.id].Match)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.Prs[r.id].Match,
		LogTerm: term,
	}

	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends a request vote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: term,
	}

	r.msgs = append(r.msgs, msg)
}

func (r *Raft) resetElectionTimeout() {
	r.randElectionTimeout = r.electionTimeout + r.random.Next()
	r.electionElapsed = 0
}

func (r *Raft) startElection() {
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
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	if r.electionElapsed >= r.randElectionTimeout {
		log.Infof("%s electionElapsed: %+v", r.logPrefix, r.electionElapsed)
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
	r.startNoop()

	log.Infof("%s become leader LII: %+v, entries: %+v", r.logPrefix, r.RaftLog.LastIndex(), r.RaftLog.allEntries())
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
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Infof("%s step message: %+v", r.logPrefix, m)
	log.Infof("%s term: %v, state: %+v", r.logPrefix, r.Term, r.State)

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
}

func (r *Raft) startOps(entries []*pb.Entry) {
	for _, pEntry := range entries {
		if pEntry != nil {
			entry := pb.Entry{Index: r.RaftLog.LastIndex() + 1, Term: r.Term, Data: pEntry.Data}
			r.RaftLog.entries = append(r.RaftLog.entries, entry)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	r.startOps(m.Entries)
	log.Infof("%s handle propose LII: %+v, entries: %+v", r.logPrefix, r.RaftLog.LastIndex(), r.RaftLog.allEntries())
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		r.apply()
		return
	}
	r.sendAppends()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%s handle append entries start: %+v", r.logPrefix, r.RaftLog.allEntries())
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	r.becomeFollower(m.Term, m.From)

	term, _ := r.RaftLog.Term(m.Index)
	log.Infof("%s handle append entry m.Index: %+v m.LogTerm: %d, lastindex: %+v term %+v",
		r.logPrefix, m.Index, m.LogTerm, r.RaftLog.LastIndex(), term)
	if r.RaftLog.LastIndex() < m.Index || term != m.LogTerm {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}

	index := m.Index + 1
	conflict := false
	for i := 0; i < len(m.Entries) && index <= r.RaftLog.LastIndex(); i++ {
		term, _ := r.RaftLog.Term(index)
		log.Infof("%s handle append entry i %d, index: %+v term: %+v, entry term: %+v", r.logPrefix, i, index, term, m.Entries[i].Term)
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
			entries = append(entries, *entry)
		}
	}

	log.Infof("%s handle append entries stabled %+v index %+v mid: %+v", r.logPrefix, r.RaftLog.stabled, index, entries)
	if len(entries) > 0 {
		if index <= r.RaftLog.stabled {
			if entries[len(entries)-1].Index >= r.RaftLog.stabled {
				r.RaftLog.storage.(*MemoryStorage).Append(entries[:r.RaftLog.stabled-entries[0].Index+1])
				if entries[len(entries)-1].Index > r.RaftLog.stabled {
					r.RaftLog.entries = append(r.RaftLog.entries, entries[r.RaftLog.stabled-entries[0].Index+1:]...)
				}
			} else {
				r.RaftLog.storage.(*MemoryStorage).Append(entries)
			}
		} else {
			r.RaftLog.entries = append(r.RaftLog.entries, entries...)
		}
	}

	if conflict {
		r.RaftLog.stabled = index - 1
	}

	var lastNewIndex uint64
	if len(entries) == 0 {
		lastNewIndex = index - 1
	} else {
		lastNewIndex = r.RaftLog.LastIndex()
	}
	r.Prs[r.id] = &Progress{lastNewIndex, lastNewIndex + 1}

	//r.RaftLog.storage.(*MemoryStorage).Append(stableEnts)
	//r.RaftLog.entries = append(r.RaftLog.entries, entries...)

	log.Infof("%s handle append entries end commit: %+v %+v", r.logPrefix, r.RaftLog.committed, r.RaftLog.allEntries())

	if len(entries) > 0 {
		msg.Index = entries[len(entries)-1].Index
	}
	msg.Reject = false
	r.msgs = append(r.msgs, msg)

	if m.Commit > r.RaftLog.committed {
		if m.Commit <= lastNewIndex {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = lastNewIndex
		}
	}

	if r.RaftLog.committed > r.RaftLog.stabled {
		r.RaftLog.stabled = r.RaftLog.committed
	}

	r.apply()
}

func (r *Raft) handleAppendEntriesRsp(m pb.Message) {
	// Your Code Here (2A).
	if m.Term == r.Term {
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From] = &Progress{m.Index, m.Index + 1}
		}

		log.Infof("%s handle append response progress: %+v commit: %d", r.logPrefix, r.Prs[m.From], r.RaftLog.committed)

		count := 1
		index := r.Prs[m.From].Match
		for peer, prs := range r.Prs {
			if peer != r.id && prs.Match > r.RaftLog.committed {
				count++
				if prs.Match < index { // the min index for commit
					index = prs.Match
				}
			}
		}
		term, _ := r.RaftLog.Term(index)
		if count > len(r.Prs)/2 && term == r.Term {
			r.RaftLog.committed = index
			if r.RaftLog.committed > r.RaftLog.stabled {
				r.RaftLog.stabled = r.RaftLog.committed
			}
			r.sendAppends()
			r.apply()
		}
	} else {

	}
}

func (r *Raft) apply() {
	if r.RaftLog.committed > r.RaftLog.applied {
		var entries []pb.Entry
		for _, entry := range r.RaftLog.entries {
			if entry.Index > r.RaftLog.applied && entry.Index <= r.RaftLog.committed {
				entries = append(entries, entry)
			}
		}
		err := r.RaftLog.storage.(*MemoryStorage).Append(entries)
		if len(r.RaftLog.entries) > 0 {
			log.Infof("%s apply entries committed: %+v entries[0].Index: %+v", r.logPrefix, r.RaftLog.committed, r.RaftLog.entries[0].Index)
			truncateIndex := r.RaftLog.committed - r.RaftLog.entries[0].Index + 1
			r.RaftLog.entries = r.RaftLog.entries[truncateIndex:]
		}
		log.Infof("%s apply end committed %d entries: %+v ", r.logPrefix, r.RaftLog.committed, r.RaftLog.entries)

		if err == nil {
			//r.RaftLog.applied = r.RaftLog.committed
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%s handle heartbeat commit %+v entries: %+v ", r.logPrefix, r.RaftLog.committed, r.RaftLog.allEntries())
	log.Infof("%s handle heartbeat match: %+v ", r.logPrefix, r.Prs[r.id])
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}

	term, _ := r.RaftLog.Term(m.Index)
	if r.RaftLog.LastIndex() < m.Index || term != m.LogTerm {
		r.msgs = append(r.msgs, msg)
		return
	}

	if m.Commit > r.RaftLog.committed {
		if m.Commit <= r.Prs[r.id].Match {
			r.RaftLog.committed = m.Commit
		} else {
			r.RaftLog.committed = r.Prs[r.id].Match
		}
	}

	r.apply()

	if r.RaftLog.committed < m.Commit {
		r.msgs = append(r.msgs, msg)
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {

	log.Infof("%s handleRequestVote Term: %+v Vote: %+v entries %+v", r.logPrefix, r.Term, r.Vote, r.RaftLog.allEntries())

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
	log.Infof("%s handleRequestVote lastIndex: %+v lastLogTerm: %+v ", r.logPrefix, lastIndex, lastLogTerm)
	log.Infof("%s handleRequestVote m.LogTerm: %+v m.Index: %+v ", r.logPrefix, m.LogTerm, m.Index)
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

	log.Infof("%s votes: %+v peer count: %+v", r.logPrefix, r.votes, len(r.Prs))
	if count > len(r.Prs)/2 {
		r.becomeLeader()
		r.sendHeartbeats()
		r.resetElectionTimeout()
	}

	if len(r.votes) == len(r.Prs) && count <= len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// handleHeartBeatRsp handle HeartBeat RPC response
func (r *Raft) handleHeartbeatRsp(m pb.Message) {
	r.sendAppend(m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
