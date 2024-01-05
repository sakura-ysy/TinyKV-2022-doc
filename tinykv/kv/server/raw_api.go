package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// 获取reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}

	// 获取 value
	cf := req.Cf
	key := req.Key
	value, err := reader.GetCF(cf,key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}

	// 返回响应
	notFound := false
	if value == nil {
		notFound = true
	}
	resp := &kvrpcpb.RawGetResponse{
		Value:value,
		NotFound: notFound,
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	// 构造 Modify 与 batch
	put := storage.Put{
		Key: req.Key,
		Value: req.Value,
		Cf: req.Cf,
	}
	modi := storage.Modify{
		Data: put,
	}
	batch := []storage.Modify{modi}

	// 传入 Write
	err := server.storage.Write(req.Context,batch)

	// 返回响应
	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 构造 Modify 与 batch
	del := storage.Delete{
		Key: req.Key,
		Cf: req.Cf,
	}
	modi := storage.Modify{
		Data: del,
	}
	batch := []storage.Modify{modi}

	// 传入 Write
	err := server.storage.Write(req.Context,batch)

	// 返回响应
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	// 获取 reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	// 迭代
	iter := reader.IterCF(req.Cf)
	var kvs []*kvrpcpb.KvPair
	cnt := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		val, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		cnt ++
		if cnt == req.Limit {
			break
		}
	}
	// 返回响应
	resp := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	return resp, nil
}
