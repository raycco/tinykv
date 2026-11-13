package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	rsp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	server.Latches.WaitForLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}

	if lock != nil && lock.IsLockedFor(req.Key, req.Version, rsp) {
		return rsp, nil
	}

	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}

	rsp.NotFound = (len(value) == 0)
	rsp.Value = value
	return rsp, nil
}

type PrewriteKeyError struct {
	Error *kvrpcpb.KeyError
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	rsp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	keysToLatch := make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keysToLatch = append(keysToLatch, mutation.Key)
	}

	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	keyErr := new(PrewriteKeyError)
	for _, mutation := range req.Mutations {
		lock, err := mvccTxn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}

		if lock.IsLockedFor(mutation.Key, req.StartVersion, keyErr) {
			rsp.Errors = append(rsp.Errors, keyErr.Error)
			return rsp, nil
		}

		recentWrite, commitTs, err := mvccTxn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}

		if recentWrite != nil && recentWrite.StartTS <= req.StartVersion && commitTs >= req.StartVersion {
			conflict := &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: recentWrite.StartTS,
				Key:        mutation.Key,
				Primary:    req.PrimaryLock,
			}
			rsp.Errors = append(rsp.Errors, &kvrpcpb.KeyError{Conflict: conflict})
			return rsp, nil
		}

		switch mutation.Op {
		case kvrpcpb.Op_Put:
			mvccTxn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			mvccTxn.DeleteValue(mutation.Key)
		}

		newLock := mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		}
		mvccTxn.PutLock(mutation.Key, &newLock)
	}

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	server.Latches.Validate(mvccTxn, keysToLatch)
	return rsp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	rsp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	log.Debugf("req: %+v", req)
	for _, key := range req.Keys {

		currWrite, commitTs, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		log.Debugf("repeatWrite: %+v", currWrite)
		if currWrite != nil && currWrite.StartTS == req.StartVersion && commitTs == req.CommitVersion {
			if currWrite.Kind == mvcc.WriteKindRollback {
				// rolled back
				rsp.Error = &kvrpcpb.KeyError{Abort: "transaction was rolled back"}
				return rsp, nil
			}
			continue
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock == nil {
			return rsp, nil
		}

		log.Debugf("lock: %+v", lock)
		if !lock.IsLockedFor(key, req.StartVersion, rsp) {
			return rsp, nil
		} else {
			rsp.Error = nil
		}

		if lock.Ts < req.StartVersion {
			rsp.Error = &kvrpcpb.KeyError{Retryable: "wait and retry"}
			return rsp, nil
		}

		write := &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind}

		mvccTxn.PutWrite(key, req.CommitVersion, write)
		mvccTxn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	server.Latches.Validate(mvccTxn, req.Keys)
	return rsp, nil
}

type ScanKeyError struct {
	Error *kvrpcpb.KeyError
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)

	log.Debugf("req: %+v", req)
	scanner := mvcc.NewScanner(req.StartKey, mvccTxn)
	defer scanner.Close()

	count := 0
	for {
		if count >= int(req.Limit) {
			break
		}

		key, value, err := scanner.Next()
		var keyErr *kvrpcpb.KeyError
		if err != nil {
			keyErr = &kvrpcpb.KeyError{Abort: err.Error()}
		}
		if key == nil && value == nil {
			break
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			keyErr = &kvrpcpb.KeyError{Abort: err.Error()}
		}

		var scanErr ScanKeyError
		if lock != nil && lock.IsLockedFor(key, req.Version, scanErr) {
			keyErr = scanErr.Error
		}

		rsp.Pairs = append(rsp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
			Error: keyErr,
		})

		count++
	}

	return rsp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	log.Debugf("req: %+v", req)

	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	recentWrite, commitTs, err := mvccTxn.MostRecentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	log.Debugf("recent write: %+v", recentWrite)
	if recentWrite != nil && recentWrite.StartTS == req.LockTs {
		if recentWrite.Kind != mvcc.WriteKindRollback {
			rsp.CommitVersion = commitTs
		} else {
			rsp.CommitVersion = 0
		}
		return rsp, nil
	}

	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock != nil {
		log.Debugf("current ts: %+v, lock ts: %+v", mvcc.PhysicalTime(req.CurrentTs), mvcc.PhysicalTime(lock.Ts))
	}

	if lock == nil || req.LockTs != lock.Ts {
		rsp.Action = kvrpcpb.Action_LockNotExistRollback
		write := &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback}
		mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, write)
	} else {
		if mvcc.PhysicalTime(req.CurrentTs) >= mvcc.PhysicalTime(lock.Ts)+lock.Ttl {
			rsp.Action = kvrpcpb.Action_TTLExpireRollback
			mvccTxn.DeleteValue(req.PrimaryKey)
			mvccTxn.DeleteLock(req.PrimaryKey)
			write := &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback}
			mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, write)
		} else {
			rsp.LockTtl = lock.Ttl - (mvcc.PhysicalTime(req.CurrentTs) - mvcc.PhysicalTime(lock.Ts))
			rsp.Action = kvrpcpb.Action_NoAction
			return rsp, nil
		}
	}

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	server.Latches.Validate(mvccTxn, [][]byte{req.PrimaryKey})
	return rsp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	log.Debugf("req: %+v", req)

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}

		log.Debugf("lock: %+v", lock)

		if lock != nil && !lock.IsLockedFor(key, req.StartVersion, rsp) {
			return rsp, nil
		} else {
			rsp.Error = nil
		}

		recentWrite, commitTs, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}

		log.Debugf("recent write: %+v", recentWrite)
		if recentWrite != nil {
			if recentWrite.StartTS == req.StartVersion && commitTs > req.StartVersion {
				rsp.Error = &kvrpcpb.KeyError{Abort: "Abort"}
				return rsp, nil
			}

			if recentWrite.StartTS == req.StartVersion && recentWrite.Kind == mvcc.WriteKindRollback {
				continue
			}
		}

		write := &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback}

		mvccTxn.DeleteValue([]byte(key))
		if lock != nil && lock.Ts == req.StartVersion {
			mvccTxn.DeleteLock([]byte(key))
		}
		mvccTxn.PutWrite([]byte(key), req.StartVersion, write)
	}

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	server.Latches.Validate(mvccTxn, req.Keys)
	return rsp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	log.Debugf("req: %+v", req)

	klPairs, err := mvcc.AllLocksForTxn(mvccTxn)
	if err != nil {
		return nil, err
	}

	var keysToLatch [][]byte
	for _, klPair := range klPairs {
		keysToLatch = append(keysToLatch, klPair.Key)
	}

	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	for _, klPair := range klPairs {
		log.Debugf("key lock pair: %+v", klPair)
		if req.CommitVersion > 0 { // commit
			recentWrite, commitTs, err := mvccTxn.MostRecentWrite(klPair.Key)
			if err != nil {
				return nil, err
			}

			if recentWrite != nil && recentWrite.StartTS == klPair.Lock.Ts && commitTs >= klPair.Lock.Ts {
				continue
			}
			mvccTxn.DeleteLock(klPair.Key)
			write := &mvcc.Write{StartTS: req.StartVersion, Kind: klPair.Lock.Kind}
			mvccTxn.PutWrite(klPair.Key, req.CommitVersion, write)
		} else { // rollback
			recentWrite, commitTs, err := mvccTxn.MostRecentWrite(klPair.Key)
			if err != nil {
				return nil, err
			}

			if recentWrite != nil && recentWrite.StartTS == klPair.Lock.Ts && commitTs >= klPair.Lock.Ts {
				continue
			}

			mvccTxn.DeleteValue(klPair.Key)
			mvccTxn.DeleteLock(klPair.Key)
			write := &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback}
			mvccTxn.PutWrite(klPair.Key, req.StartVersion, write)
		}
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			rsp.RegionError = regionErr.RequestErr
			return rsp, nil
		}
		return nil, err
	}
	server.Latches.Validate(mvccTxn, keysToLatch)
	return rsp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
