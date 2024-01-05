package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	NextKey []byte
	Txn *MvccTxn
	Iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scanner := & Scanner{
		NextKey: startKey,
		Txn: txn,
		Iter: iter,
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.Iter
	if !iter.Valid() {
		return nil, nil, nil
	}
	key := scan.NextKey
	// 指定 key、ts 下的最新 write
	iter.Seek(EncodeKey(key, scan.Txn.StartTS))
	if !iter.Valid(){
		return nil, nil, nil
	}
	item := iter.Item()
	gotKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(gotKey)
	if !bytes.Equal(userKey, key) {
		// nextKey 错误，更正重试
		scan.NextKey = userKey
		return scan.Next()
	}
	// 找到了，确定新的 nextKey
	for ; ; {
		iter.Next()
		if !iter.Valid() {
			break
		}
		item2 := iter.Item()
		gotKey2 := item2.KeyCopy(nil)
		userKey2 := DecodeUserKey(gotKey2)
		if !bytes.Equal(userKey2, key) {
			scan.NextKey = userKey2
			break
		}
	}

	wValue, err := item.ValueCopy(nil)
	if err != nil {
		return key, nil, err
	}

	write, err := ParseWrite(wValue)
	if err != nil {
		return key, nil, err
	}
	if write == nil {
		return key, nil, nil
	}

	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}

	value, err := scan.Txn.Reader.GetCF(engine_util.CfDefault,EncodeKey(key, write.StartTS))
	if err != nil {
		return key, nil	, err
	}
	return key, value, nil
}
