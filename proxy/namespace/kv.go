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
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"

	"golang.org/x/net/context"
)

type kvProxy struct {
	kv  pb.KVServer
	pfx []byte
}

func NewKvProxy(kv pb.KVServer, pfx string) pb.KVServer { return &kvProxy{kv: kv, pfx: []byte(pfx)} }

func (p *kvProxy) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp, err := p.kv.Range(ctx, p.prefixRangeRequest(r))
	if err != nil {
		return nil, err
	}
	p.unprefixRangeResponse(resp)
	return resp, nil
}

func (p *kvProxy) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	resp, err := p.kv.Put(ctx, p.prefixPutRequest(r))
	if err != nil {
		return nil, err
	}
	p.unprefixPutResponse(resp)
	return resp, nil
}

func (p *kvProxy) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := p.kv.DeleteRange(ctx, p.prefixDeleteRangeRequest(r))
	if err != nil {
		return nil, err
	}
	p.unprefixDeleteRangeResponse(resp)
	return resp, nil
}

func (p *kvProxy) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	resp, err := p.kv.Txn(ctx, p.prefixTxnRequest(r))
	if err != nil {
		return nil, err
	}
	p.unprefixTxnResponse(resp)
	return resp, nil
}

func (p *kvProxy) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	return p.kv.Compact(ctx, r)
}

func (p *kvProxy) prefixRangeRequest(r *pb.RangeRequest) *pb.RangeRequest {
	req := *r
	req.Key, req.RangeEnd = p.prefixInterval(req.Key, req.RangeEnd)
	return &req
}

func (p *kvProxy) unprefixRangeResponse(resp *pb.RangeResponse) {
	for i := range resp.Kvs {
		resp.Kvs[i].Key = resp.Kvs[i].Key[:len(p.pfx)]
	}
}

func (p *kvProxy) prefixPutRequest(r *pb.PutRequest) *pb.PutRequest {
	req := *r
	req.Key = make([]byte, len(p.pfx)+len(r.Key))
	copy(req.Key[copy(req.Key, p.pfx):], r.Key)
	return &req
}

func (p *kvProxy) unprefixPutResponse(resp *pb.PutResponse) {
	if resp.PrevKv != nil {
		resp.PrevKv.Key = resp.PrevKv.Key[:len(p.pfx)]
	}
}

func (p *kvProxy) prefixDeleteRangeRequest(r *pb.DeleteRangeRequest) *pb.DeleteRangeRequest {
	req := *r
	req.Key, req.RangeEnd = p.prefixInterval(req.Key, req.RangeEnd)
	return &req
}

func (p *kvProxy) unprefixDeleteRangeResponse(resp *pb.DeleteRangeResponse) {
	for i := range resp.PrevKvs {
		resp.PrevKvs[i].Key = resp.PrevKvs[i].Key[:len(p.pfx)]
	}
}

func (p *kvProxy) prefixTxnRequest(r *pb.TxnRequest) *pb.TxnRequest {
	txn := *r
	txn.Compare = make([]*pb.Compare, len(txn.Compare))
	for i, cmp := range r.Compare {
		ccopy := *cmp
		txn.Compare[i] = &ccopy
		if cmp.Key == nil {
			continue
		}
		txn.Compare[i].Key = make([]byte, len(p.pfx)+len(cmp.Key))
		copy(txn.Compare[i].Key[copy(cmp.Key, p.pfx):], cmp.Key)
	}
	txn.Success = p.prefixRequestOps(r.Success)
	txn.Failure = p.prefixRequestOps(r.Failure)
	return &txn
}

func (p *kvProxy) unprefixTxnResponse(resp *pb.TxnResponse) {
	for _, r := range resp.Responses {
		switch tv := r.Response.(type) {
		case *pb.ResponseOp_ResponseRange:
			if tv.ResponseRange != nil {
				p.unprefixRangeResponse(tv.ResponseRange)
			}
		case *pb.ResponseOp_ResponsePut:
			if tv.ResponsePut != nil {
				p.unprefixPutResponse(tv.ResponsePut)
			}
		case *pb.ResponseOp_ResponseDeleteRange:
			if tv.ResponseDeleteRange != nil {
				p.unprefixDeleteRangeResponse(tv.ResponseDeleteRange)
			}
		default:
			// empty union?
		}
	}

}

func (p *kvProxy) prefixRequestOps(ops []*pb.RequestOp) []*pb.RequestOp {
	pfxOps := make([]*pb.RequestOp, len(ops))
	for i := range ops {
		pfxOps[i] = &pb.RequestOp{Request: ops[i].Request}
		switch tv := pfxOps[i].Request.(type) {
		case *pb.RequestOp_RequestRange:
			if tv.RequestRange != nil {
				pfxOps[i].Request = &pb.RequestOp_RequestRange{
					RequestRange: p.prefixRangeRequest(tv.RequestRange),
				}
			}
		case *pb.RequestOp_RequestPut:
			if tv.RequestPut != nil {
				pfxOps[i].Request = &pb.RequestOp_RequestPut{
					RequestPut: p.prefixPutRequest(tv.RequestPut),
				}
			}
		case *pb.RequestOp_RequestDeleteRange:
			if tv.RequestDeleteRange != nil {
				pfxOps[i].Request = &pb.RequestOp_RequestDeleteRange{
					RequestDeleteRange: p.prefixDeleteRangeRequest(tv.RequestDeleteRange),
				}
			}
		default:
			// empty union?
		}
	}
	return pfxOps
}

func (p *kvProxy) prefixInterval(key, end []byte) (pfxKey []byte, pfxEnd []byte) {
	pfxKey = make([]byte, len(p.pfx)+len(key))
	copy(pfxKey[:copy(pfxKey, p.pfx)], key)

	if len(end) == 1 && end[0] == 0 {
		// the edge of the keyspace
		pfxEnd = make([]byte, len(p.pfx))
		copy(pfxEnd, p.pfx)
		ok := false
		for i := len(pfxEnd); i >= 0; i-- {
			if pfxEnd[i]++; pfxEnd[i] != 0 {
				ok = true
				break
			}
		}
		if !ok {
			// 0xff..ff => 0x00
			pfxEnd = []byte{0}
		}
	} else if len(end) >= 1 {
		pfxKey = make([]byte, len(p.pfx)+len(key))
		copy(pfxKey[:copy(pfxKey, p.pfx)], key)
	}

	return pfxKey, pfxEnd
}
