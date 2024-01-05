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
	modify := storage.Modify{
		Data : storage.Put{
			Key: EncodeKey(key, ts),
			Cf: engine_util.CfWrite,
			Value: write.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	reader := txn.Reader
	value, err := reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	lock, err := ParseLock(value)
	if err != nil || lock == nil{
		return nil, nil
	}
	return lock, err
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key: key,
			Cf: engine_util.CfLock,
			Value: lock.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf: engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	reader := txn.Reader
	wIter := reader.IterCF(engine_util.CfWrite)
	// 找到 commitTs <= ts 最新 Write
	wIter.Seek(EncodeKey(key, txn.StartTS))  // key 增序，ts 降序
	if !wIter.Valid() {
		return nil, nil
	}

	wItem := wIter.Item()
	gotKey := DecodeUserKey(wItem.KeyCopy(nil))
	if !bytes.Equal(gotKey, key) {
		return nil, nil
	}

	wValue, err := wItem.ValueCopy(nil)
	write, _ := ParseWrite(wValue)
	if err != nil || write.Kind != WriteKindPut {
		return nil, err
	}

	value, err := reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	return value, err
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Put{
			Key: EncodeKey(key, txn.StartTS),
			Cf: engine_util.CfDefault,
			Value: value,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	modify := storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS),
			Cf: engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	reader := txn.Reader
	wIter := reader.IterCF(engine_util.CfWrite)
	for wIter.Seek(EncodeKey(key,TsMax)); wIter.Valid(); wIter.Next() {
		wItem := wIter.Item()
		gotKey := DecodeUserKey(wItem.KeyCopy(nil))
		if !bytes.Equal(gotKey, key){
			return nil, 0, nil
		}
		wValue, err := wItem.ValueCopy(nil)
		if wValue == nil || err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(wValue)
		if write == nil || err != nil {
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(wItem.KeyCopy(nil)), nil
		}
		if write.StartTS < txn.StartTS {
			break
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	reader := txn.Reader
	wIter := reader.IterCF(engine_util.CfWrite)
	wIter.Seek(EncodeKey(key,TsMax))
	if !wIter.Valid(){
		return nil, 0, nil
	}

	wItem := wIter.Item()
	gotKey := DecodeUserKey(wItem.KeyCopy(nil))
	if !bytes.Equal(gotKey, key){
		return nil, 0, nil
	}

	wValue, err := wItem.ValueCopy(nil)
	if wValue == nil || err != nil {
		return nil, 0, err
	}

	write, err := ParseWrite(wValue)
	if write == nil || err != nil {
		return nil, 0, err
	}
	return write, decodeTimestamp(wItem.KeyCopy(nil)), nil
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
