package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// 定义engine和conf
	engine *engine_util.Engines
	config *config.Config
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// dbPath直接从conf中取就行
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath,"kv")
	raftPath := path.Join(dbPath, "raft")

	// 创建磁盘目录与DB对象
	kvEngine := engine_util.CreateDB(kvPath,false)
	raftEngine := engine_util.CreateDB(raftPath, true)

	store := StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine,raftEngine,kvPath,raftPath),
		config: conf,
	}
	return &store
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kvTxn: kvTxn,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 初始化txn
	kvTxn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneReader(kvTxn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 根据 modify 选择性调用 PutCF 或 DeleteCF
	for _,b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			key := put.Key
			value := put.Value
			cf := put.Cf
			err := engine_util.PutCF(s.engine.Kv,cf,key,value)
			if err != nil{
				return err
			}
			break
		case storage.Delete:
			del := b.Data.(storage.Delete)
			key := del.Key
			cf := del.Cf
			err := engine_util.DeleteCF(s.engine.Kv,cf,key)
			if err != nil{
				return nil
			}
			break
		}
	}
	return nil
}

/**
	实现StorageReader接口
 */

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error){
	value, err := engine_util.GetCFFromTxn(s.kvTxn,cf,key)
	// key 不存在
	if err == badger.ErrKeyNotFound {
		return nil, nil  // 测试要求 err 为 nil，而不是 KeyNotFound，否则没法过
	}
	return value,err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator{
	// 直接用现有的
	return engine_util.NewCFIterator(cf,s.kvTxn)
}

func (s *StandAloneReader) Close() {
	// 关闭 txn，和 commit 对应
	s.kvTxn.Discard()
	return
}