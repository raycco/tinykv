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
	stabled, _ := storage.LastIndex()
	return &RaftLog{storage: storage, applied: 0, committed: 0, stabled: stabled}
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
	firstIndex, _ := l.storage.FirstIndex()
	lastIndex, _ := l.storage.LastIndex()
	entries, _ := l.storage.Entries(firstIndex, lastIndex+1)
	log.Debugf("all entries storage %+v memory %+v", entries, l.entries)
	m := make(map[string]pb.Entry)
	var result []pb.Entry
	for _, p := range entries {
		if _, ok := m[p.String()]; !ok {
			m[p.String()] = p
			result = append(result, p)
		}
	}
	for _, p := range l.entries {
		if _, ok := m[p.String()]; !ok {
			m[p.String()] = p
			result = append(result, p)
		}
	}
	return result
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled > l.LastIndex() {
		return []pb.Entry{}
	}

	lastIndex, _ := l.storage.LastIndex()
	var entries []pb.Entry
	if l.stabled < lastIndex {
		entries, _ = l.storage.Entries(l.stabled+1, lastIndex+1)
		entries = append(entries, l.entries...)
	} else {
		for _, entry := range l.entries {
			if entry.Index > l.stabled {
				entries = append(entries, entry)
			}
		}
	}

	if len(entries) == 0 {
		return []pb.Entry{}
	}
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	entries, err := l.storage.Entries(l.applied+1, l.committed+1)
	if err != nil {
		return nil
	}
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var lastIndex uint64
	if len(l.entries) >= 1 {
		lastIndex = l.entries[len(l.entries)-1].Index
	} else {
		lastIndex, _ = l.storage.LastIndex()
	}
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	term, err := l.storage.Term(i)
	if err == nil {
		return term, err
	}

	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}

	return 0, nil
}

func (l *RaftLog) ArrIndex(i uint64) int {
	// Your Code Here (2A).
	arrIndex := 0
	for _, entry := range l.entries {
		if entry.Index == i {
			return arrIndex
		}
	}

	return 0
}
