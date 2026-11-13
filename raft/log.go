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
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("init log first index err: %+v", err)
	}

	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Panicf("init log last index err: %+v", err)
	}

	entries, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		log.Panicf("init log entries firstIdx=%+v lastIdx=%+v err: %+v", firstIdx, lastIdx, err)
	}

	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Panicf("init log initial state err: %+v", err)
	}

	return &RaftLog{
		storage:         storage,
		applied:         firstIdx - 1,
		committed:       hardState.Commit,
		stabled:         lastIdx,
		entries:         entries,
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	firstIdx, err := l.storage.FirstIndex()
	if err != nil {
		log.Panicf("all entries first index err: %+v", err)
	}

	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("all entries last index err: %+v", err)
	}

	log.Infof("all entries: storage fisrtIdx=%+v and lastIdx=%+v, stabled=%+v", firstIdx, lastIdx, l.stabled)

	var stabledEnts []pb.Entry
	if firstIdx <= l.stabled && l.stabled <= lastIdx {
		stabledEnts, err = l.storage.Entries(firstIdx, l.stabled+1)
		if err != nil {
			log.Panicf("all entries storage entries err: %+v", err)
		}
	}

	entries := make([]pb.Entry, len(stabledEnts))
	copy(entries, stabledEnts)
	entries = append(entries, l.entries...)

	ents := l.trimDummyEnts(entries)

	return ents
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled > l.LastIndex() {
		return []pb.Entry{}
	}

	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("unstable entries last index err: %+v", err)
	}

	var entries []pb.Entry
	if l.stabled < lastIdx && len(l.entries) == 0 {
		entries, err = l.storage.Entries(l.stabled+1, lastIdx+1)
		if err != nil {
			log.Panicf("unstable entries storage entries err: %+v", err)
		}
	} else {
		for _, entry := range l.entries {
			if entry.Index > l.stabled {
				entries = append(entries, entry)
			}
		}
	}

	log.Infof("unstable entries: stabled=%+v, committed=%+v, storage lastIdx=%+v, lastIdx=%+v, entries = %+v",
		l.stabled, l.committed, lastIdx, l.LastIndex(), entries)

	if len(entries) == 0 {
		return []pb.Entry{}
	}

	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("next entries last index err: %+v", err)
	}

	var stabledEnts []pb.Entry
	if l.applied < lastIdx {
		if lastIdx > l.committed {
			stabledEnts, err = l.storage.Entries(l.applied+1, l.committed+1)
		} else {
			stabledEnts, err = l.storage.Entries(l.applied+1, lastIdx+1)
		}

		if err != nil {
			log.Panicf("next entries storage entries err: %+v", err)
		}
	}

	// 直接append到stabledEnts会修改MemoryStorage中的内容
	entries := make([]pb.Entry, len(stabledEnts))
	copy(entries, stabledEnts)
	for _, entry := range l.entries {
		if entry.Index <= l.committed && entry.Index > l.applied {
			entries = append(entries, entry)
		}
	}

	ents = l.trimDummyEnts(entries)

	log.Infof("next entries: applied=%+v, committed=%+v, storage lastIdx=%+v, lastIdx=%+v",
		l.applied, l.committed, lastIdx, l.LastIndex())

	return ents
}

func (l *RaftLog) FirstIndex() (uint64, error) {
	return l.storage.FirstIndex()
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var lastIdx uint64
	if len(l.entries) > 0 {
		lastIdx = l.entries[len(l.entries)-1].Index
	} else {
		var err error
		lastIdx, err = l.storage.LastIndex()
		if err != nil {
			log.Panicf("last index err: %+v", err)
		}
	}

	if l.pendingSnapshot != nil &&
		l.pendingSnapshot.Metadata != nil &&
		l.pendingSnapshot.Metadata.Index > lastIdx {
		lastIdx = l.pendingSnapshot.Metadata.Index
	}
	return lastIdx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if l.pendingSnapshot != nil &&
		l.pendingSnapshot.Metadata != nil &&
		i <= l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("term last index err: %+v", err)
	}
	if (i <= l.stabled && l.stabled <= lastIdx) || len(l.entries) == 0 {
		return l.storage.Term(i)
	} else {
		offset := l.entries[0].Index
		if i < offset {
			return 0, ErrCompacted
		}
		if int(i-offset) >= len(l.entries) {
			return 0, ErrUnavailable
		}
		return l.entries[i-offset].Term, nil
	}
}

func (l *RaftLog) AdvanceApplied(applied uint64) {
	if applied > l.applied {
		l.applied = applied
	}

	if l.applied > l.committed {
		l.applied = l.committed
	}
}

func (l *RaftLog) AdvanceStabled(stabled uint64) {

	if stabled > l.stabled {
		l.stabled = stabled
	}

	// snap与append乱序可能导致stabled > storage.LastIndex()
	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("advance stabled last index err: %+v", err)
	}
	if l.stabled > lastIdx {
		l.stabled = lastIdx
	}

	if len(l.entries) > 0 {
		firstIdx := l.entries[0].Index
		lastIdx := l.entries[len(l.entries)-1].Index
		if l.stabled >= lastIdx {
			l.entries = l.entries[:0]
			return
		}

		if l.stabled >= firstIdx {
			l.entries = l.entries[l.stabled-firstIdx+1:]
		}
	}
}

func (l *RaftLog) trimDummyEnts(entries []pb.Entry) (ents []pb.Entry) {
	mapIdxEntry := make(map[uint64]pb.Entry)
	for _, entry := range entries {
		mapIdxEntry[entry.Index] = entry
	}

	var idxs []uint64
	for idx := range mapIdxEntry {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool {
		return idxs[i] < idxs[j]
	})

	for _, idx := range idxs {
		ents = append(ents, mapIdxEntry[idx])
	}
	return ents
}

func (l *RaftLog) appendEnts(entries []pb.Entry, index uint64, conflict bool) {
	firstIdx, err := l.FirstIndex()
	if err != nil {
		log.Panicf("append entries first index err: %+v", err)
	}
	if conflict && len(l.entries) > 0 && index >= firstIdx {
		// truncate log after conflict index
		if index-firstIdx > uint64(len(l.entries)) {
			l.entries = l.entries[:0]
		} else {
			l.entries = l.entries[:index-firstIdx]
		}
	}
	l.entries = append(l.entries, entries...)

	if conflict {
		l.stabled = index - 1
	}
}

func (l *RaftLog) buildAppendEnts(index uint64) []pb.Entry {

	var entries []pb.Entry
	if index > l.LastIndex() {
		return entries
	}

	lastIdx, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf("build entries last index err: %+v", err)
	}

	var stabledEnts []pb.Entry
	if index <= lastIdx {
		stabledEnts, err = l.storage.Entries(index, lastIdx+1)
		if err != nil {
			log.Panicf("build entries storage entries err: %+v", err)
		}
	}

	entries = make([]pb.Entry, len(stabledEnts))
	copy(entries, stabledEnts)
	for _, entry := range l.entries {
		if entry.Index >= index {
			entries = append(entries, entry)
		}
	}

	ents := l.trimDummyEnts(entries)
	return ents
}
