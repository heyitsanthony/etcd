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

package vauth 

import (
	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/auth"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type kvVAuth struct {
	clientv3.KV
	pfx string
}

// NewKV wraps a KV instance so that all requests
// are prefixed with a given string.
func NewKV(kv clientv3.KV, prefix string) clientv3.KV {
	return &kvVAuth{kv, prefix}
}

func (kv *kvVAuth) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	r, err := kv.Do(ctx, clientv3.OpPut(key, val, opts...))
	if err != nil {
		return nil, err
	}
	return r.Put(), nil
}

func (kv *kvVAuth) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	r, err := kv.Do(ctx, clientv3.OpGet(key, opts...))
	if err != nil {
		return nil, err
	}
	return r.Get(), nil
}

func (kv *kvVAuth) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	r, err := kv.Do(ctx, clientv3.OpDelete(key, opts...))
	if err != nil {
		return nil, err
	}
	return r.Del(), nil
}

func (kv *kvVAuth) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	if !kv.hasPermission(ctx, op) {
		// TODO: check server if up-to-date on etcd server
		return r, rpctypes.ErrPermissionDenied
	}
	r, err := kv.KV.Do(ctx, kv.protectOp(op))
	if err != nil {
		return r, err
	}

	panic("stub")
	return r, nil
}

func (kv *kvVAuth) hasPermission(ctx context.Context, op clientv3.Op) bool {
	// get user from token (if not expired)
	// inspect op
	panic("check context header")
	// IsPutPermitted checks put permission of the user
	IsPutPermitted(authInfo *AuthInfo, key []byte) error
	// IsRangePermitted checks range permission of the user
	IsRangePermitted(authInfo *AuthInfo, key, rangeEnd []byte) error
	// IsDeleteRangePermitted checks delete-range permission of the user
	IsDeleteRangePermitted(authInfo *AuthInfo, key, rangeEnd []byte) error
	// TODO: need to wrap other interfaces
}

func (kv *kvVAuth) protectOp(op clientv3.Op) clientv3.Op
	permCmps := []clientv3.Cmp{
		// confirm revision for auth has not changed since cached
		clientv3.Compare(clientv3.ModRevision(op), "=", kv.cacheRev),
	}
	miss := []clientv3.Op{
		// fetch new auth data, in authpb format
		clientv3.OpGet(kv.pfx+"/roles"),
		clientv3.OpGet(kv.pfx+"/users"),
	}
	return clientv3.OpTxn(permCmps, op, miss)
}

type txnVAuth struct {
	clientv3.Txn
	ctx context.Context
	kv *kvVAuth
}

func (kv *kvVAuth) Txn(ctx context.Context) clientv3.Txn {
	return &txnVAuth{kv.KV.Txn(ctx), ctx, kv}
}

func (txn *txnVAuth) If(cs ...clientv3.Cmp) clientv3.Txn {
	txn.Txn = txn.Txn.If(txn.kv.prefixCmps(cs)...)
	return txn
}

func (txn *txnVAuth) Then(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Then(txn.kv.prefixOps(ops)...)
	return txn
}

func (txn *txnVAuth) Else(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Else(txn.kv.prefixOps(ops)...)
	return txn
}

func (txn *txnVAuth) Commit() (*clientv3.TxnResponse, error) {
	resp, err := kv.Do(ctx, txnOp)
	if err != nil {
		return nil, err
	}
	return resp.Txn(), nil
}
