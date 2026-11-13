package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey  []byte
	txn       *MvccTxn
	iterWrite engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := new(Scanner)
	scan.startKey = startKey
	scan.txn = txn

	scan.iterWrite = txn.Reader.IterCF(engine_util.CfWrite)
	scan.iterWrite.Seek(startKey)

	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iterWrite.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iterWrite.Valid() {
		return nil, nil, nil
	}
	var lastKey []byte
	var deleteKey []byte
	var lastCommitTs uint64
	var lastWrite *Write
	for ; scan.iterWrite.Valid(); scan.iterWrite.Next() {
		writeItem := scan.iterWrite.Item()
		lastKey = DecodeUserKey(writeItem.Key())
		if bytes.Equal(lastKey, deleteKey) {
			continue
		}
		lastCommitTs = decodeTimestamp(writeItem.Key())
		writeBytes, err := writeItem.Value()
		if err != nil {
			return nil, nil, err
		}
		lastWrite, err = ParseWrite(writeBytes)
		if err != nil {
			return nil, nil, err
		}

		if scan.txn.StartTS >= lastCommitTs {
			if lastWrite.Kind == WriteKindDelete {
				deleteKey = lastKey
				lastKey = make([]byte, 0)
				continue
			}
			break
		}
	}

	for ; scan.iterWrite.Valid(); scan.iterWrite.Next() {
		writeItem := scan.iterWrite.Item()

		key := DecodeUserKey(writeItem.Key())
		if !bytes.Equal(lastKey, key) {
			scan.iterWrite.Seek(key)
			break
		}

		commitTs := decodeTimestamp(writeItem.Key())
		log.Debugf("key: %+v, commitTs %+v", key, commitTs)

		writeBytes, err := writeItem.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(writeBytes)
		if err != nil {
			return nil, nil, err
		}

		if scan.txn.StartTS >= commitTs && commitTs > lastCommitTs && write.Kind == WriteKindPut {
			lastCommitTs = commitTs
			lastWrite = write
		}
	}

	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(lastKey, lastWrite.StartTS))
	if err != nil {
		return nil, nil, err
	}

	log.Debugf("lastKey: %+v, value %+v, lastWrite: %+v", lastKey, value, lastWrite)

	return lastKey, value, nil
}
