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
	"math"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 持久化的entry
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
	// 所有未被 compact 的 entry, 包括持久化与非持久化
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// 上一条追加的index，用于 follower 更新 committed
	lastAppend uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panicf("storage must not be nil")
	}
	log := &RaftLog{
		storage: storage,
	}

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)

	hardState, _, _ := storage.InitialState()
	log.committed = hardState.Commit
	log.applied = firstIndex - 1
	log.stabled = lastIndex
	log.entries = entries // newLog 时还没有非持久化entry
	log.pendingSnapshot = nil
	log.lastAppend = math.MaxInt64
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	remainedIndex, _ := l.storage.FirstIndex() // 在此之前的均被压缩
	if len(l.entries) > 0 {
		if remainedIndex > l.LastIndex() {
			l.entries = nil
		} else if remainedIndex >= l.FirstIndex() {
			l.entries = l.entries[remainedIndex-l.FirstIndex():]
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.stabled < firstIndex {
			return l.entries
		}
		if l.stabled-firstIndex >= uint64(len(l.entries)-1) {
			return make([]pb.Entry, 0)
		}
		return l.entries[l.stabled-firstIndex+1:]
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex := l.FirstIndex()
	appliedIndex := l.applied
	commitedIndex := l.committed
	if len(l.entries) > 0 {
		if appliedIndex >= firstIndex-1 && commitedIndex >= firstIndex-1 && appliedIndex < commitedIndex && commitedIndex <= l.LastIndex() {
			return l.entries[appliedIndex-firstIndex+1 : commitedIndex-firstIndex+1]
		}
	}
	return make([]pb.Entry, 0)
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		index, _ := l.storage.FirstIndex()
		return index
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()
		if i >= firstIndex && i <= lastIndex {
			return l.entries[i-firstIndex].Term, nil
		}
	}

	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	return 0, err
}

func (l *RaftLog) appliedTo(toApply uint64) {
	l.applied = toApply
}

// 参考 etcd commitTo
func (l *RaftLog) commitTo(toCommit uint64) {
	// never decrease commit
	if l.committed < toCommit {
		//if l.LastIndex() < toCommit {
		//	log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
		//}
		l.committed = toCommit
	}
}

// 参考 etcd findConflictByTerm
// findConflictByTerm takes an (index, term) pair (indicating a conflicting log
// entry on a leader/follower during an append) and finds the largest index in
// log l with a term <= `term` and an index <= `index`. If no such index exists
// in the log, the log's first index is returned.
//
// The index provided MUST be equal to or less than l.lastIndex(). Invalid
// inputs log a warning and the input index is returned.
func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		log.Panicf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm", index, li)
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

// 用于追加日志
//func (l *RaftLog) append(es ...pb.Entry) uint64 {
//	if len(es) == 0 {
//		return l.LastIndex()
//	}
//	if after := es[0].Index - 1; after < l.committed {
//		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
//	}
//	for _, e := range es {
//		l.entries = append(l.entries, e)
//	}
//	return l.LastIndex()
//}
