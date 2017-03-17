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
	"context"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type watchProxy struct {
	w   pb.WatchServer
	ctx context.Context
}

func NewWatchProxy(ctx context.Context, w pb.WatchServer) pb.WatchServer {
	return &watchProxy{ctx: ctx, w: w}
}

func (wp *watchProxy) Watch(stream pb.Watch_WatchServer) (err error) {
	panic("whatever")
}
