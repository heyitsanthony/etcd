// Copyright 2016 The etcd Authors
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

package grpcproxy

import (
	"fmt"

	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/proxy/grpcproxy/cache"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

type kvProxy struct {
	kv    clientv3.KV
	cache cache.Cache
}

func newKvProxy(c *clientv3.Client) *kvProxy {
	return &kvProxy{
		kv:    c.KV,
		cache: cache.NewCache(cache.DefaultMaxEntries),
	}
}

type kvProxyServer kvProxy
func NewKvProxyServer(c *clientv3.Client) pb.KVServer { return (*kvProxyServer)(newKvProxy(c)) }

func (p *kvProxyServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error)  {
	return ((*kvProxy)(p)).get(ctx, r)
}

func (p *kvProxyServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	return ((*kvProxy)(p)).put(ctx, r)
}

func (p *kvProxyServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	return ((*kvProxy)(p)).deleteRange(ctx, r)
}

func (p *kvProxyServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	return ((*kvProxy)(p)).compact(ctx, r)
}

func (p *kvProxyServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return ((*kvProxy)(p)).txn(ctx, r)
}

type kvProxyClient kvProxy
func NewKvProxyClient(c *clientv3.Client) pb.KVClient { return (*kvProxyClient)(newKvProxy(c)) }

func (p *kvProxyClient) Range(ctx context.Context, r *pb.RangeRequest, copts ...grpc.CallOption) (*pb.RangeResponse, error)  {
	return ((*kvProxy)(p)).get(ctx, r, copts...)
}

func (p *kvProxyClient) Put(ctx context.Context, r *pb.PutRequest, copts ...grpc.CallOption) (*pb.PutResponse, error) {
	return ((*kvProxy)(p)).put(ctx, r, copts...)
}

func (p *kvProxyClient) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest, copts ...grpc.CallOption) (*pb.DeleteRangeResponse, error) {
	return ((*kvProxy)(p)).deleteRange(ctx, r, copts...)
}

func (p *kvProxyClient) Compact(ctx context.Context, r *pb.CompactionRequest, copts ...grpc.CallOption) (*pb.CompactionResponse, error) {
	return ((*kvProxy)(p)).compact(ctx, r, copts...)
}

func (p *kvProxyClient) Txn(ctx context.Context, r *pb.TxnRequest, copts ...grpc.CallOption) (*pb.TxnResponse, error) {
	return ((*kvProxy)(p)).txn(ctx, r, copts...)
}

func (p *kvProxy) get(ctx context.Context, r *pb.RangeRequest, copts ...grpc.CallOption) (*pb.RangeResponse, error) {
	if r.Serializable {
		resp, err := p.cache.Get(r)
		switch err {
		case nil:
			return resp, nil
		case cache.ErrCompacted:
			return nil, err
		}
	}

	resp, err := p.kv.Do(ctx, RangeRequestToOp(r))
	if err != nil {
		return nil, err
	}

	// cache linearizable as serializable
	req := *r
	req.Serializable = true
	gresp := (*pb.RangeResponse)(resp.Get())
	p.cache.Add(&req, gresp)

	return gresp, nil
}

func (p *kvProxy) put(ctx context.Context, r *pb.PutRequest, copts ...grpc.CallOption) (*pb.PutResponse, error) {
	p.cache.Invalidate(r.Key, nil)
	resp, err := p.kv.Do(ctx, PutRequestToOp(r, copts...))
	return (*pb.PutResponse)(resp.Put()), err
}

func (p *kvProxy) deleteRange(ctx context.Context, r *pb.DeleteRangeRequest, copts ...grpc.CallOption) (*pb.DeleteRangeResponse, error) {
	p.cache.Invalidate(r.Key, r.RangeEnd)
	resp, err := p.kv.Do(ctx, DelRequestToOp(r))
	return (*pb.DeleteRangeResponse)(resp.Del()), err
}

func (p *kvProxy) txnToCache(reqs []*pb.RequestOp, resps []*pb.ResponseOp) {
	for i := range resps {
		switch tv := resps[i].Response.(type) {
		case *pb.ResponseOp_ResponsePut:
			p.cache.Invalidate(reqs[i].GetRequestPut().Key, nil)
		case *pb.ResponseOp_ResponseDeleteRange:
			rdr := reqs[i].GetRequestDeleteRange()
			p.cache.Invalidate(rdr.Key, rdr.RangeEnd)
		case *pb.ResponseOp_ResponseRange:
			req := *(reqs[i].GetRequestRange())
			req.Serializable = true
			p.cache.Add(&req, tv.ResponseRange)
		}
	}
}

func (p *kvProxy) txn(ctx context.Context, r *pb.TxnRequest, copts ...grpc.CallOption) (*pb.TxnResponse, error) {
	txn := p.kv.Txn(ctx)
	cmps := make([]clientv3.Cmp, len(r.Compare))
	thenops := make([]clientv3.Op, len(r.Success))
	elseops := make([]clientv3.Op, len(r.Failure))

	for i := range r.Compare {
		cmps[i] = (clientv3.Cmp)(*r.Compare[i])
	}

	for i := range r.Success {
		thenops[i] = requestOpToOp(r.Success[i])
	}

	for i := range r.Failure {
		elseops[i] = requestOpToOp(r.Failure[i])
	}

	resp, err := txn.If(cmps...).Then(thenops...).Else(elseops...).Commit()

	if err != nil {
		return nil, err
	}
	// txn may claim an outdated key is updated; be safe and invalidate
	for _, cmp := range r.Compare {
		p.cache.Invalidate(cmp.Key, nil)
	}
	// update any fetched keys
	if resp.Succeeded {
		p.txnToCache(r.Success, resp.Responses)
	} else {
		p.txnToCache(r.Failure, resp.Responses)
	}
	return (*pb.TxnResponse)(resp), nil
}

func (p *kvProxy) compact(ctx context.Context, r *pb.CompactionRequest, copts ...grpc.CallOption) (*pb.CompactionResponse, error) {
	var opts []clientv3.CompactOption
	if r.Physical {
		opts = append(opts, clientv3.WithCompactPhysical())
	}

	resp, err := p.kv.Compact(ctx, r.Revision, opts...)
	if err == nil {
		p.cache.Compact(r.Revision)
	}

	return (*pb.CompactionResponse)(resp), err
}

func requestOpToOp(union *pb.RequestOp) clientv3.Op {
	switch tv := union.Request.(type) {
	case *pb.RequestOp_RequestRange:
		if tv.RequestRange != nil {
			return RangeRequestToOp(tv.RequestRange)
		}
	case *pb.RequestOp_RequestPut:
		if tv.RequestPut != nil {
			return PutRequestToOp(tv.RequestPut)
		}
	case *pb.RequestOp_RequestDeleteRange:
		if tv.RequestDeleteRange != nil {
			return DelRequestToOp(tv.RequestDeleteRange)
		}
	}
	panic("unknown request")
}

func RangeRequestToOp(r *pb.RangeRequest) clientv3.Op {
	opts := []clientv3.OpOption{}
	if len(r.RangeEnd) != 0 {
		opts = append(opts, clientv3.WithRange(string(r.RangeEnd)))
	}
	opts = append(opts, clientv3.WithRev(r.Revision))
	opts = append(opts, clientv3.WithLimit(r.Limit))
	opts = append(opts, clientv3.WithSort(
		clientv3.SortTarget(r.SortTarget),
		clientv3.SortOrder(r.SortOrder)),
	)
	opts = append(opts, clientv3.WithMaxCreateRev(r.MaxCreateRevision))
	opts = append(opts, clientv3.WithMinCreateRev(r.MinCreateRevision))
	opts = append(opts, clientv3.WithMaxModRev(r.MaxModRevision))
	opts = append(opts, clientv3.WithMinModRev(r.MinModRevision))

	if r.Serializable {
		opts = append(opts, clientv3.WithSerializable())
	}

	return clientv3.OpGet(string(r.Key), opts...)
}

func PutRequestToOp(r *pb.PutRequest, copts ...grpc.CallOption) clientv3.Op {
	opts := []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(r.Lease))}
	opts = append(opts, clientv3.WithCallOptions(copts...))
	fmt.Println("MAKING OPPUT")
	return clientv3.OpPut(string(r.Key), string(r.Value), opts...)
}

func DelRequestToOp(r *pb.DeleteRangeRequest) clientv3.Op {
	opts := []clientv3.OpOption{}
	if len(r.RangeEnd) != 0 {
		opts = append(opts, clientv3.WithRange(string(r.RangeEnd)))
	}
	if r.PrevKv {
		opts = append(opts, clientv3.WithPrevKV())
	}
	return clientv3.OpDelete(string(r.Key), opts...)
}
