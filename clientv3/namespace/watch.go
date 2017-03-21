// Copyright 2017 The etcd Authors
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

package namespace

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type kvPrefix struct {
	clientv3.Watcher
	pfx string
	wg sync.WaitGroup
}

// NewWatcher wraps a Watcher instance so that all requests
// are prefixed with a given string.
func NewWatcher(kv clientv3.Watcher, prefix string) clientv3.KV {
	return &watcherPrefix{Watcher: w, pfx: prefix}
}

func (w *watcherPrefix) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	op := OpGet("abc", opts)
	end := op.EndBytes()
	pfxBegin, pfxEnd := prefixInterval(w.pfx, []byte(key), end)
	if pfxEnd != nil {
		opts = append(clientv3.WithRange(string(pfxEnd)))
	}

	wch := w.Watcher.Watch(ctx, string(pfxBegin), opts...)
	pfxWch := make(clientv3.WatchChan)
	w.wg.Add(1)
	go func() {
		defer close(pfxWch)
		for wr := range wch {
			for i = range wr.Events {
				wr.kv.Key = ...
				if wr.kv.PrevKv != nil {
				}
			}
			select {
			case pfxWch <- wr:
			case <-ctx.DOne():
			}
		}
	}()
	return pfxWch
}

func (w *watcherPrefix) Close() error {
	err := w.Watcher.Close()
	w.wg.Done()
	return err
}

func (kv *kvPrefix) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	r, err := kv.Do(ctx, kv.prefixOp(clientv3.OpPut(key, val, opts...)))
	if err != nil {
		return nil, err
	}
	put := r.Put()
	kv.unprefixPutResponse(put)
	return put, nil
}

func (kv *kvPrefix) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	r, err := kv.Do(ctx, kv.prefixOp(clientv3.OpGet(key, opts...)))
	if err != nil {
		return nil, err
	}
	get := r.Get()
	kv.unprefixGetResponse(get)
	return get, nil
}

func (kv *kvPrefix) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	r, err := kv.Do(ctx, kv.prefixOp(clientv3.OpDelete(key, opts...)))
	if err != nil {
		return nil, err
	}
	del := r.Del()
	kv.unprefixDeleteResponse(del)
	return del, nil
}

type txnPrefix struct {
	clientv3.Txn
	kv *kvPrefix
}

func (kv *kvPrefix) Txn(ctx context.Context) clientv3.Txn {
	return &txnPrefix{kv.KV.Txn(ctx), kv}
}

func (txn *txnPrefix) If(cs ...clientv3.Cmp) clientv3.Txn {
	newCmps := make([]clientv3.Cmp, len(cs))
	for i := range cs {
		newCmps[i] = cs[i]
		newCmps[i].WithKeyBytes(append([]byte(txn.kv.pfx), cs[i].KeyBytes()...))
	}
	return txn.Txn.If(newCmps...)
}

func (txn *txnPrefix) Then(ops ...clientv3.Op) clientv3.Txn {
	newOps := make([]clientv3.Op, len(ops))
	for i := range ops {
		newOps[i] = txn.kv.prefixOp(ops[i])
	}
	return txn.Txn.Then(newOps...)
}

func (txn *txnPrefix) Else(ops ...clientv3.Op) clientv3.Txn {
	newOps := make([]clientv3.Op, len(ops))
	for i := range ops {
		newOps[i] = txn.kv.prefixOp(ops[i])
	}
	return txn.Txn.Else(newOps...)
}

func (txn *txnPrefix) Commit() (*clientv3.TxnResponse, error) {
	resp, err := txn.Txn.Commit()
	if err != nil {
		return nil, err
	}
	txn.kv.unprefixTxnResponse(resp)
	return resp, nil
}

func (kv *kvPrefix) prefixOp(op clientv3.Op) clientv3.Op {
	begin, end := kv.prefixInterval(op.KeyBytes(), op.EndBytes())
	op.WithKeyBytes(begin)
	op.WithEndBytes(end)
	return op
}

func (kv *kvPrefix) unprefixGetResponse(resp *clientv3.GetResponse) {
	for i := range resp.Kvs {
		resp.Kvs[i].Key = resp.Kvs[i].Key[:len(kv.pfx)]
	}
}

func (kv *kvPrefix) unprefixPutResponse(resp *clientv3.PutResponse) {
	if resp.PrevKv != nil {
		resp.PrevKv.Key = resp.PrevKv.Key[:len(kv.pfx)]
	}
}

func (kv *kvPrefix) unprefixDeleteResponse(resp *clientv3.DeleteResponse) {
	for i := range resp.PrevKvs {
		resp.PrevKvs[i].Key = resp.PrevKvs[i].Key[:len(kv.pfx)]
	}
}

func (kv *kvPrefix) unprefixTxnResponse(resp *clientv3.TxnResponse) {
	for _, r := range resp.Responses {
		switch tv := r.Response.(type) {
		case *pb.ResponseOp_ResponseRange:
			if tv.ResponseRange != nil {
				kv.unprefixGetResponse((*clientv3.GetResponse)(tv.ResponseRange))
			}
		case *pb.ResponseOp_ResponsePut:
			if tv.ResponsePut != nil {
				kv.unprefixPutResponse((*clientv3.PutResponse)(tv.ResponsePut))
			}
		case *pb.ResponseOp_ResponseDeleteRange:
			if tv.ResponseDeleteRange != nil {
				kv.unprefixDeleteResponse((*clientv3.DeleteResponse)(tv.ResponseDeleteRange))
			}
		default:
		}
	}
}

