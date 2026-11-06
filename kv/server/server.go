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
	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}

	if lock.IsLockedFor(req.Key, req.Version, rsp) {
		return rsp, nil
	}

	wg := server.Latches.AcquireLatches([][]byte{req.Key})
	if wg != nil {
		rsp.Error = &kvrpcpb.KeyError{Retryable: "wait and retry"}
		return rsp, nil
	}

	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		rsp.NotFound = true
	}
	rsp.Value = value
	server.Latches.ReleaseLatches([][]byte{req.Key})
	return rsp, nil
}

type PrewriteKeyError struct {
	Error *kvrpcpb.KeyError
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	rsp := new(kvrpcpb.PrewriteResponse)
	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	keyErr := new(PrewriteKeyError)
	keys := make([][]byte, 0)
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

		if recentWrite != nil {
			if recentWrite.StartTS <= req.StartVersion && commitTs >= req.StartVersion {
				conflict := &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: recentWrite.StartTS,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				}
				rsp.Errors = append(rsp.Errors, &kvrpcpb.KeyError{Conflict: conflict})
				return rsp, nil
			}
		}

		keys = append(keys, mutation.Key)
	}

	wg := server.Latches.AcquireLatches(keys)
	if wg != nil {
		rsp.Errors = append(rsp.Errors, &kvrpcpb.KeyError{Retryable: "wait and retry"})
		return rsp, nil
	}

	for _, mutation := range req.Mutations {
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			mvccTxn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			mvccTxn.DeleteValue(mutation.Key)
		}

		lock := mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		}
		mvccTxn.PutLock(mutation.Key, &lock)
	}

	err := server.storage.Write(req.Context, mvccTxn.Writes())
	server.Latches.ReleaseLatches(keys)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	rsp := new(kvrpcpb.CommitResponse)
	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	writes := make(map[string]*mvcc.Write)
	for _, key := range req.Keys {

		repeatWrite, commitTs, err := mvccTxn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if repeatWrite != nil && repeatWrite.StartTS == req.StartVersion && commitTs == req.CommitVersion {
			continue
		}

		recentWrite, _, err := mvccTxn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}

		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock == nil && recentWrite != nil {
			rsp.Error = &kvrpcpb.KeyError{Locked: nil}
			return rsp, nil
		}

		if !lock.IsLockedFor(key, req.StartVersion, rsp) {
			return rsp, nil
		} else {
			rsp.Error = nil
		}

		if lock.Ts < req.StartVersion {
			rsp.Error = &kvrpcpb.KeyError{Retryable: "wait and retry"}
			return rsp, nil
		}

		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		}
		writes[string(key)] = write
	}

	wg := server.Latches.AcquireLatches(req.Keys)
	if wg != nil {
		rsp.Error = &kvrpcpb.KeyError{Retryable: "wait and retry"}
		return rsp, nil
	}

	for key, write := range writes {
		mvccTxn.PutWrite([]byte(key), req.CommitVersion, write)
		mvccTxn.DeleteLock([]byte(key))
	}

	err := server.storage.Write(req.Context, mvccTxn.Writes())
	server.Latches.ReleaseLatches(req.Keys)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.ScanResponse)
	reader, _ := server.storage.Reader(req.Context)
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
	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.LockTs)
	log.Debugf("req: %+v", req)

	recentWrite, commitTs, err := mvccTxn.MostRecentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	log.Debugf("recentWrite: %+v", recentWrite)

	lock, err := mvccTxn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if recentWrite != nil && commitTs > 0 {
		if lock != nil && lock.Ttl <= 8 {

		} else {
			rsp.CommitVersion = commitTs
		}
		return rsp, nil
	} else if recentWrite == nil {
		rsp.Action = kvrpcpb.Action_LockNotExistRollback
	}

	log.Debugf("lock: %+v", lock)
	if lock != nil {
		if req.CurrentTs > lock.Ts && lock.Ttl <= 8 {
			rsp.LockTtl = lock.Ttl
			rsp.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			rsp.Action = kvrpcpb.Action_NoAction
			return rsp, nil
		}
	}

	wg := server.Latches.AcquireLatches([][]byte{req.PrimaryKey})
	if wg != nil {
		return rsp, nil
	}

	mvccTxn.DeleteValue(req.PrimaryKey)
	mvccTxn.DeleteLock(req.PrimaryKey)
	mvccTxn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})

	err = server.storage.Write(req.Context, mvccTxn.Writes())
	server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

type WriteInfo struct {
	write *mvcc.Write
	lock  *mvcc.Lock
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.BatchRollbackResponse)
	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	log.Debugf("req: %+v", req)
	writes := make(map[string]WriteInfo)
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

		log.Debugf("recentWrite: %+v", recentWrite)
		if recentWrite != nil {
			if recentWrite.StartTS == req.StartVersion && commitTs > req.StartVersion {
				rsp.Error = &kvrpcpb.KeyError{Abort: "Abort"}
				return rsp, nil
			}

			if recentWrite.StartTS == req.StartVersion && recentWrite.Kind == mvcc.WriteKindRollback {
				continue
			}
		}

		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		}
		writeInfo := WriteInfo{write, lock}
		writes[string(key)] = writeInfo
		log.Debugf("writeInfo: %+v", writeInfo)
	}

	wg := server.Latches.AcquireLatches(req.Keys)
	if wg != nil {
		rsp.Error = &kvrpcpb.KeyError{Retryable: "wait and retry"}
		return rsp, nil
	}

	for key, info := range writes {
		mvccTxn.DeleteValue([]byte(key))
		if info.lock != nil && info.lock.Ts == req.StartVersion {
			mvccTxn.DeleteLock([]byte(key))
		}
		mvccTxn.PutWrite([]byte(key), req.StartVersion, info.write)
	}

	err := server.storage.Write(req.Context, mvccTxn.Writes())
	server.Latches.ReleaseLatches(req.Keys)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	rsp := new(kvrpcpb.ResolveLockResponse)
	reader, _ := server.storage.Reader(req.Context)
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	log.Debugf("req: %+v", req)

	klPairs, err := mvcc.AllLocksForTxn(mvccTxn)
	if err != nil {
		return nil, err
	}

	for _, klPair := range klPairs {
		log.Debugf("klPair: %+v", klPair)
		if req.CommitVersion > 0 {
			recentWrite, commitTs, err := mvccTxn.MostRecentWrite(klPair.Key)
			if err != nil {
				return nil, err
			}

			if recentWrite != nil && recentWrite.StartTS == klPair.Lock.Ts && commitTs >= klPair.Lock.Ts {
				continue
			}
			mvccTxn.DeleteLock(klPair.Key)
			mvccTxn.PutWrite(klPair.Key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: klPair.Lock.Kind})
		} else {
			recentWrite, commitTs, err := mvccTxn.MostRecentWrite(klPair.Key)
			if err != nil {
				return nil, err
			}

			if recentWrite != nil && recentWrite.StartTS == klPair.Lock.Ts && commitTs >= klPair.Lock.Ts {
				continue
			}

			mvccTxn.DeleteValue(klPair.Key)
			mvccTxn.DeleteLock(klPair.Key)
			mvccTxn.PutWrite(klPair.Key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
		}
	}
	err = server.storage.Write(req.Context, mvccTxn.Writes())
	if err != nil {
		return nil, err
	}
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
