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

package leasing

import (
	"strings"
	"sync"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	concurrency "github.com/coreos/etcd/clientv3/concurrency"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"golang.org/x/net/context"
)

type leasingKV struct {
	cl       *v3.Client
	kv       v3.KV
	pfx      string
	session  *concurrency.Session
	leases   leaseCache
	ctx      context.Context
	cancel   context.CancelFunc
	header   *server.ResponseHeader
	maxRev   int64
	sessionc chan struct{}
}

type leaseCache struct {
	monitorRevocation map[string]time.Time
	entries           map[string]*leaseInfo
	mu                sync.Mutex
	flag              int
}

type leaseInfo struct {
	waitc    chan struct{}
	response *v3.GetResponse
	revision int64
}

// NewleasingKV wraps a KV instance so that all requests are wired through a leasing protocol.
func NewleasingKV(cl *v3.Client, leasingprefix string) (v3.KV, error) {
	s, err := concurrency.NewSession(cl)
	if err != nil {
		return nil, err
	}
	sessionc := make(chan struct{})
	close(sessionc)
	cctx, cancel := context.WithCancel(cl.Ctx())
	kv := leasingKV{cl: cl, kv: cl.KV, pfx: leasingprefix, session: s, leases: leaseCache{monitorRevocation: make(map[string]time.Time),
		entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel, maxRev: 1, sessionc: sessionc}
	go kv.monitorSession(cl, 0)
	return &kv, nil
}

// NewleasingKVTTL wraps a KV instance so that all requests are wired through a leasing protocol and uses a TTL to keep session alive
func NewleasingKVTTL(cl *v3.Client, leasingprefix string, ttl int) (v3.KV, error) {
	s, err := concurrency.NewSession(cl, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	sessionc := make(chan struct{})
	close(sessionc)
	cctx, cancel := context.WithCancel(cl.Ctx())
	kv := leasingKV{cl: cl, kv: cl.KV, pfx: leasingprefix, session: s, leases: leaseCache{monitorRevocation: make(map[string]time.Time),
		entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel, maxRev: 1, sessionc: sessionc}
	return &kv, nil
}

func (lkv *leasingKV) monitorSession(cl *v3.Client, ttl int) {
	for {
		<-lkv.session.Done()
		select {
		case <-lkv.ctx.Done():
			return
		default:
			lkv.leases.mu.Lock()
			lkv.sessionc = make(chan struct{})
			lkv.leases.clearCache()
			lkv.leases.mu.Unlock()
			s, err := startNewSession(cl, ttl)
			if err != nil {
				continue
			}
			lkv.initializeSession(s)
		}
	}
}

func (lkv *leasingKV) updateCache(ctx context.Context, key, val string, op v3.Op) (*v3.PutResponse, error) {
	wc, rev := lkv.leases.openWaitChannel(key)
	if rev == 0 {
		return nil, nil
	}
	respUpd, errUpd := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(op).Commit()
	if errUpd != nil {
		lkv.leases.deleteKeyInCache(key)
		close(wc)
		return nil, errUpd
	}
	if lkv.leases.checkInCache(key) != nil {
		lkv.leases.updateCacheResp(key, val, respUpd.Header)
	}
	close(wc)
	if !respUpd.Succeeded {
		return nil, nil
	}
	putResp := (*v3.PutResponse)(respUpd.Responses[0].GetResponsePut())
	putResp.Header = respHeaderPopulate(respUpd.Header)
	return putResp, nil
}

func (lkv *leasingKV) watchforLKDel(ctx context.Context, key string, rev int64) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wch := lkv.cl.Watch(cctx, lkv.pfx+key, v3.WithRev(rev+1))
	for resp := range wch {
		for _, ev := range resp.Events {
			if ev.Type == v3.EventTypeDelete {
				return
			}
		}
	}
}

func (lkv *leasingKV) put(ctx context.Context, key, val string, op v3.Op) (*v3.PutResponse, error) {
	for ctx.Err() == nil {
		<-lkv.sessionc
		respPut, err := lkv.updateCache(ctx, key, val, op)
		if respPut != nil || err != nil {
			return respPut, err
		}
		txn := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(op)
		resp, err := txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease())).Commit()
		if err != nil {
			return nil, err
		}
		if resp.Succeeded {
			response := (*v3.PutResponse)(resp.Responses[0].GetResponsePut())
			response.Header = respHeaderPopulate(resp.Header)
			return response, err
		}
		lkv.watchforLKDel(ctx, key, resp.Header.Revision)
	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) Put(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
	r, err := lkv.Do(ctx, v3.OpPut(key, val, opts...))
	if err != nil {
		return nil, err
	}
	return r.Put(), nil
}

const (
	acquireLKAgainDur int = 2
)

func (lkv *leasingKV) monitorLease(ctx context.Context, key string, resp *v3.TxnResponse, rev int64) {
	nCtx, cancel := context.WithCancel(lkv.ctx)
	defer cancel()
	for nCtx.Err() == nil {
		wch := lkv.cl.Watch(nCtx, lkv.pfx+key, v3.WithRev(resp.Header.Revision+1))
		for resp := range wch {
			for _, ev := range resp.Events {
				if string(ev.Kv.Value) == "REVOKE" {
					lkv.leases.trackRevokedLK(key)
					txn := lkv.kv.Txn(nCtx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(v3.OpDelete(lkv.pfx + key))
					delResp, err := txn.Commit()
					if delResp.Succeeded {
						lkv.leases.deleteKeyInCache(key)
						return
					}
					if err != nil || !delResp.Succeeded {
						continue
					}
				}
			}
		}
	}
}

func (lkv *leasingKV) acquireLeaseRPC(ctx context.Context, key string, op v3.Op) (*v3.TxnResponse, error) {
	txn := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	resp, err := txn.Then((op), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.leaseID()))).Else(op).Commit()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (lkv *leasingKV) acquireLease(ctx context.Context, key string, op v3.Op) (*v3.TxnResponse, error) {
	lkv.leases.mu.Lock()
	lr, ok := lkv.leases.monitorRevocation[key]
	lkv.leases.mu.Unlock()
	if ok {
		timeElasped := lr.Minute()*60 + lr.Second()
		currTime := time.Now().Local().Minute()*60 + (time.Now().Local().Second())
		if timeElasped < currTime {
			return lkv.acquireLeaseRPC(ctx, key, op)
		}
		resp, err := lkv.kv.Txn(ctx).Then(op).Commit()
		txnResp := &v3.TxnResponse{
			Header:    respHeaderPopulate(resp.Header),
			Succeeded: false,
			Responses: []*server.ResponseOp{(resp.Responses[0])},
		}
		return txnResp, err
	}
	return lkv.acquireLeaseRPC(ctx, key, op)
}

func (lkv *leasingKV) get(ctx context.Context, key string, op v3.Op) (*v3.GetResponse, error) {
	if len(op.RangeBytes()) > 0 || !(op.IsKeysOnly()) && op.Rev() > 0 {
		r, err := lkv.kv.Do(ctx, op)
		if err != nil {
			return nil, err
		}
		return r.Get(), nil
	}
	var gresp *v3.GetResponse
	getResp, err := lkv.leases.returnCachedResp(ctx, key, op), ctx.Err()
	if getResp != nil || err != nil {
		return getResp, err
	}
	if op.IsSerializable() {
		return lkv.kv.Get(ctx, key, v3.WithSerializable())
	}
	resp, err := lkv.acquireLease(ctx, key, op)
	if err != nil {
		return nil, err
	}
	if resp.Succeeded {
		getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
		getResp.Header = respHeaderPopulate(resp.Header)
		lkv.addToCache(getResp, key)
		go lkv.monitorLease(ctx, key, resp, resp.Header.Revision)
		gresp = lkv.leases.returnCachedResp(ctx, key, op)
	} else {
		gresp = (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	}
	return gresp, nil
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if lkv.checkOpenSessionChannel() {
		r, err := lkv.Do(ctx, v3.OpGet(key, opts...))
		if err != nil {
			return nil, err
		}
		return r.Get(), nil
	}
	return lkv.kv.Get(ctx, key, opts...)
}

func (lkv *leasingKV) deleteKey(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	wc, rev := lkv.leases.openWaitChannel(key)
	if rev == 0 {
		return nil, nil
	}
	respDel, errDel := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(op).Commit()
	if errDel != nil {
		lkv.leases.deleteKeyInCache(key)
		close(wc)
		return nil, errDel
	}
	if !respDel.Succeeded {
		close(wc)
		return nil, nil
	}
	lkv.leases.deleteKeyInCache(key)
	close(wc)
	delResp := (*v3.DeleteResponse)(respDel.Responses[0].GetResponseDeleteRange())
	delResp.Header = respHeaderPopulate(respDel.Header)
	return delResp, nil
}

const (
	modRev    int = 2
	createRev int = 1
)

func (lkv *leasingKV) deleteRange(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	for ctx.Err() == nil {
		endKey, leasingendKey := string(op.RangeBytes()), v3.GetPrefixRangeEnd(lkv.pfx+key)
		var wc [](chan struct{})
		resp, err := lkv.kv.Get(ctx, key, v3.WithRange(endKey))
		if err != nil {
			return nil, err
		}
		getRespLK, err := lkv.kv.Get(ctx, lkv.pfx+key, v3.WithRange(leasingendKey))
		if err != nil {
			return nil, err
		}
		for i := range resp.Kvs {
			gkey := string(resp.Kvs[i].Key)
			if _, ok := lkv.leases.entries[gkey]; !ok {
				if err := lkv.revokeLease(ctx, gkey); err != nil {
					return nil, err
				}
			} else {
				wcKey, _ := lkv.leases.openWaitChannel(gkey)
				wc = append(wc, wcKey)
			}
		}
		getResp, err := lkv.kv.Get(ctx, key, v3.WithRange(endKey))
		if err != nil {
			return nil, err
		}
		maxRev, maxRevLK := maxRevision(getResp, modRev), maxRevision(getRespLK, createRev)
		txn1 := lkv.kv.Txn(ctx).If(v3.Compare(v3.ModRevision(key).WithRange(endKey), "<", maxRev+1),
			v3.Compare(v3.CreateRevision(lkv.pfx+key).WithRange(leasingendKey), "<", maxRevLK+1))
		delRangeResp, err := txn1.Then(v3.OpDelete(key, v3.WithRange(endKey))).Commit()
		if err != nil {
			closeWaitChannel(wc)
			return nil, err
		}
		if !delRangeResp.Succeeded {
			closeWaitChannel(wc)
			continue
		}
		for i := range getResp.Kvs {
			lkv.leases.deleteKeyInCache(string(getResp.Kvs[i].Key))
		}
		delResp := (*v3.DeleteResponse)(delRangeResp.Responses[0].GetResponseDeleteRange())
		delResp.Header = respHeaderPopulate(delRangeResp.Header)
		return delResp, nil
	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) delete(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	<-lkv.sessionc
	if len(op.RangeBytes()) > 0 {
		return lkv.deleteRange(ctx, key, op)
	}
	for ctx.Err() == nil {
		respDel, err := lkv.deleteKey(ctx, key, op)
		if respDel != nil || err != nil {
			return respDel, err
		}
		txn := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(op)
		resp, err := txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease())).Commit()
		if err != nil {
			return nil, err
		}
		if resp.Succeeded {
			response := (*v3.DeleteResponse)(resp.Responses[0].GetResponseDeleteRange())
			response.Header = respHeaderPopulate(resp.Header)
			return response, err
		}
		lkv.watchforLKDel(ctx, key, resp.Header.Revision)
	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	r, err := lkv.Do(ctx, v3.OpDelete(key, opts...))
	if err != nil {
		return nil, err
	}
	return r.Del(), nil
}

type txnLeasing struct {
	v3.Txn
	lkv  *leasingKV
	ctx  context.Context
	cs   []v3.Cmp
	opst []v3.Op
	opse []v3.Op
}

func (lkv *leasingKV) Txn(ctx context.Context) v3.Txn {
	return &txnLeasing{lkv.kv.Txn(ctx), lkv, ctx, make([]v3.Cmp, 0), make([]v3.Op, 0), make([]v3.Op, 0)}
}

func (txn *txnLeasing) If(cs ...v3.Cmp) v3.Txn {
	for i := range cs {
		txn.cs = append(txn.cs, cs[i])
	}
	return txn
}

func (txn *txnLeasing) Then(ops ...v3.Op) v3.Txn {
	for i := range ops {
		txn.opst = append(txn.opst, ops[i])
	}
	return txn
}

func (txn *txnLeasing) Else(ops ...v3.Op) v3.Txn {
	for i := range ops {
		txn.opse = append(txn.opse, ops[i])
	}
	return txn
}

func (txn *txnLeasing) Commit() (*v3.TxnResponse, error) {
	for txn.ctx.Err() == nil {
		var txnResp *v3.TxnResponse
		cacheBool, serverTxnBool := txn.allCmpsfromCache()
		if !serverTxnBool {
			opArray := txn.gatherAllOps(txn.defOpArray(cacheBool))
			if ok, txnResp, err := txn.noOps(opArray, cacheBool); ok {
				if err != nil {
					return nil, err
				}
				return txnResp, nil
			}
			for _, ch := range txn.lkv.leases.blockKeys(opArray, waitReleaseChan) {
				select {
				case <-ch:
				case <-txn.ctx.Done():
					return nil, txn.ctx.Err()
				}
			}
			txn.lkv.leases.mu.Lock()
			responseArray, ok := txn.cacheOpArray(opArray)
			txn.lkv.leases.mu.Unlock()
			if ok {
				if txn.lkv.checkOpenSessionChannel() {
					cacheResp, _ := txn.lkv.allInCache(responseArray, cacheBool)
					return cacheResp, nil
				}
				return txn.lkv.cl.Txn(txn.ctx).If(txn.cs...).Then(txn.opst...).Else(txn.opse...).Commit()
			}
			serverTxnBool = !serverTxnBool
		}
		<-txn.lkv.sessionc
		allOps := append(txn.opst, txn.opse...)
		txnOps := txn.gatherAllOps(allOps)
		wcs := txn.lkv.leases.blockKeys(txnOps, acquireChan)
		for {
			elseOps, cmps := txn.cmpUpdate(txnOps)
			resp, err := txn.lkv.kv.Txn(txn.ctx).If(cmps...).Then(v3.OpTxn(txn.cs, txn.opst, txn.opse)).Else(elseOps...).Commit()
			if err != nil {
				for i := range cmps {
					txn.lkv.leases.deleteKeyInCache(strings.TrimPrefix(string(cmps[i].Key), txn.lkv.pfx))
				}
				return nil, err
			}
			if resp.Succeeded {
				txnResp = txn.extractResp(resp)
				txn.modifyCacheTxn(txnResp)
				closeWaitChannel(wcs)
				break
			}
			err = txn.NonOwnerRevoke(resp, elseOps, txnOps)
			if err != nil {
				return nil, err
			}
		}
		return txnResp, nil
	}
	return nil, txn.ctx.Err()
}

func (lkv *leasingKV) Compact(ctx context.Context, rev int64, opts ...v3.CompactOption) (*v3.CompactResponse, error) {
	return lkv.kv.Compact(ctx, rev, opts...)
}

func (lkv *leasingKV) Do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	return lkv.do(ctx, op)
}

func (lkv *leasingKV) do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	if op.IsGet() {
		if lkv.checkOpenSessionChannel() {
			resp, err := lkv.get(ctx, string(op.KeyBytes()), op)
			if err == nil {
				return resp.OpResponse(), nil
			}
			return v3.OpResponse{}, err
		}
		return lkv.kv.Do(ctx, op)
	}
	if op.IsPut() {
		resp, err := lkv.put(ctx, string(op.KeyBytes()), string(op.ValueBytes()), op)
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	if op.IsDelete() {
		resp, err := lkv.delete(ctx, string(op.KeyBytes()), op)
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	if op.IsTxn() {
		cmps, thenOps, elseOps := op.Txn()
		resp, err := lkv.Txn(ctx).If(cmps...).Then(thenOps...).Else(elseOps...).Commit()
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	return v3.OpResponse{}, nil
}
