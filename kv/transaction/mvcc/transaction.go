package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, ts)
	put := storage.Put{Cf: engine_util.CfWrite, Key: encodedKey, Value: write.ToBytes()}
	modify := storage.Modify{Data: put}
	txn.writes = append(txn.writes, modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}

	if len(value) > 0 {
		lock, err := ParseLock(value)
		if err != nil {
			return nil, err
		}
		return lock, nil
	}
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{Cf: engine_util.CfLock, Key: key, Value: lock.ToBytes()}
	modify := storage.Modify{Data: put}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{Cf: engine_util.CfLock, Key: key}
	modify := storage.Modify{Data: del}
	txn.writes = append(txn.writes, modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	var maxCommitTs uint64
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, key) {
			continue
		}

		commitTs := decodeTimestamp(item.Key())
		if txn.StartTS >= commitTs && commitTs > maxCommitTs {
			maxCommitTs = commitTs
		}
	}

	if maxCommitTs > 0 {
		value, err := txn.Reader.GetCF(engine_util.CfWrite, EncodeKey(key, maxCommitTs))
		if err != nil {
			return nil, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}

		return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	}

	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, txn.StartTS)
	put := storage.Put{Cf: engine_util.CfDefault, Key: encodedKey, Value: value}
	modify := storage.Modify{Data: put}
	txn.writes = append(txn.writes, modify)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	encodedKey := EncodeKey(key, txn.StartTS)
	del := storage.Delete{Cf: engine_util.CfDefault, Key: encodedKey}
	modify := storage.Modify{Data: del}
	txn.writes = append(txn.writes, modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterDefault := txn.Reader.IterCF(engine_util.CfDefault)
	defer iterDefault.Close()

	var find bool
	for ; iterDefault.Valid(); iterDefault.Next() {
		item := iterDefault.Item()
		userKey := DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, key) {
			continue
		}

		startTs := decodeTimestamp(item.Key())
		if txn.StartTS == startTs {
			find = true
			break
		}
	}

	if find {
		return txn.getWriteAndCommitTs(key, txn.StartTS, true)
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterDefault := txn.Reader.IterCF(engine_util.CfDefault)
	defer iterDefault.Close()

	var maxStartTs uint64
	for ; iterDefault.Valid(); iterDefault.Next() {
		item := iterDefault.Item()
		userKey := DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, key) {
			continue
		}

		startTs := decodeTimestamp(item.Key())
		if startTs > maxStartTs {
			maxStartTs = startTs
		}
	}

	if maxStartTs > 0 {
		return txn.getWriteAndCommitTs(key, maxStartTs, false)
	}
	return nil, 0, nil
}

func (txn *MvccTxn) getWriteAndCommitTs(key []byte, ts uint64, current bool) (*Write, uint64, error) {
	iterWrite := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterWrite.Close()

	var resWrite *Write
	var resCommitTs uint64
	for ; iterWrite.Valid(); iterWrite.Next() {
		item := iterWrite.Item()
		userKey := DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, key) {
			continue
		}

		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}

		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}

		commitTs := decodeTimestamp(item.Key())
		if current {
			if write.StartTS == ts {
				resWrite = write
				resCommitTs = decodeTimestamp(item.Key())
			}
		} else {
			if write.StartTS >= ts && commitTs > resCommitTs {
				resWrite = write
				resCommitTs = decodeTimestamp(item.Key())
			}
		}
	}

	if resCommitTs > 0 {
		return resWrite, resCommitTs, nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
