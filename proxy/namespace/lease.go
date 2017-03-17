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
	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type leaseProxy struct {
	// leaseClient handles req from LeaseGrant() that requires a lease ID.
	leaseClient pb.LeaseClient
	lessor      clientv3.Lease
}

func NewLeaseProxy(c *clientv3.Client) pb.LeaseServer {
	lp := &leaseProxy{
		leaseClient: pb.NewLeaseClient(c.ActiveConnection()),
		lessor:      c.Lease,
	}
	return lp
}

func (lp *leaseProxy) LeaseGrant(ctx context.Context, cr *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	rp, err := lp.leaseClient.LeaseGrant(ctx, cr)
	if err != nil {
		return nil, err
	}
	return rp, nil
}

func (lp *leaseProxy) LeaseRevoke(ctx context.Context, rr *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	r, err := lp.lessor.Revoke(ctx, clientv3.LeaseID(rr.ID))
	if err != nil {
		return nil, err
	}
	return (*pb.LeaseRevokeResponse)(r), nil
}

func (lp *leaseProxy) LeaseTimeToLive(ctx context.Context, rr *pb.LeaseTimeToLiveRequest) (rp *pb.LeaseTimeToLiveResponse, err error) {
	var r *clientv3.LeaseTimeToLiveResponse
	if rr.Keys {
		r, err = lp.lessor.TimeToLive(ctx, clientv3.LeaseID(rr.ID), clientv3.WithAttachedKeys())
	} else {
		r, err = lp.lessor.TimeToLive(ctx, clientv3.LeaseID(rr.ID))
	}
	if err != nil {
		return nil, err
	}
	if len(r.Keys) > 0 {
		panic("need to translate keys here")
	}
	rp = &pb.LeaseTimeToLiveResponse{
		Header:     r.ResponseHeader,
		ID:         int64(r.ID),
		TTL:        r.TTL,
		GrantedTTL: r.GrantedTTL,
		Keys:       r.Keys,
	}
	return rp, err
}

func (lp *leaseProxy) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	// XXX forward keepalive requests to server...
	for stream.Context().Err() != nil {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		panic("whatever")
	}
	return stream.Context().Err()
}
