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

package integration

import (
	"context"
	"testing"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/testutil"
)

// TestRollback1 checks rolling back a single node works.
func TestRollback1(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := NewClusterV3(t, &ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	cl := toGRPC(clus.Client(0)).Cluster
	_, err := cl.Rollback(context.TODO(), &pb.RollbackRequest{})
	testutil.AssertNil(t, err)
}

// TestRollbackTwice checks that rolling back the cluster twice
// will return an error.
func TestRollbackTwice(t *testing.T) {
	panic("whatever")
}
