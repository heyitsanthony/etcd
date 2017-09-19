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

package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/testutil"
	"github.com/coreos/etcd/version"
)

// TestReleaseUpgrade ensures that changes to master branch does not affect
// upgrade from latest etcd releases.
func TestReleaseUpgrade(t *testing.T) {
	defer testutil.AfterTest(t)

	copiedCfg := configNoTLS
	copiedCfg.execPath = lastReleaseEtcd(t)
	copiedCfg.snapCount = 3
	copiedCfg.baseScheme = "unix" // to avoid port conflict

	epc, err := newEtcdProcessCluster(&copiedCfg)
	if err != nil {
		t.Fatalf("could not start etcd process cluster (%v)", err)
	}
	defer func() {
		if errC := epc.Close(); errC != nil {
			t.Fatalf("error closing etcd processes (%v)", errC)
		}
	}()
	if err = cURLGet(epc, cURLReq{endpoint: "/version", expected: `"etcdcluster":"` + version.Cluster(version.Version)}); err != nil {
		t.Fatalf("cannot pull version (%v)", err)
	}

	cx := ctlCtx{
		t:           t,
		cfg:         configNoTLS,
		dialTimeout: 7 * time.Second,
		quorum:      true,
		epc:         epc,
	}
	var kvs []kv
	for i := 0; i < 5; i++ {
		kvs = append(kvs, kv{key: fmt.Sprintf("foo%d", i), val: "bar"})
		if err := ctlV3Put(cx, kvs[i].key, kvs[i].val, ""); err != nil {
			cx.t.Fatalf("#%d: ctlV3Put error (%v)", i, err)
		}
	}

	for i := range epc.procs {
		if err := epc.procs[i].Stop(); err != nil {
			t.Fatalf("#%d: error closing etcd process (%v)", i, err)
		}
		epc.procs[i].Config().execPath = binDir + "/etcd"
		epc.procs[i].Config().keepDataDir = true

		if err := epc.procs[i].Restart(); err != nil {
			t.Fatalf("error restarting etcd process (%v)", err)
		}
		if err := ctlV3Get(cx, []string{"--prefix", "foo"}, kvs...); err != nil {
			t.Fatalf("#%d: ctlV3Get error (%v)", i, err)
		}
	}
}

func TestReleaseUpgradeWithRestart(t *testing.T) {
	defer testutil.AfterTest(t)

	copiedCfg := configNoTLS
	copiedCfg.execPath = lastReleaseEtcd(t)
	copiedCfg.snapCount = 10
	copiedCfg.baseScheme = "unix"

	epc, err := newEtcdProcessCluster(&copiedCfg)
	if err != nil {
		t.Fatalf("could not start etcd process cluster (%v)", err)
	}
	defer func() {
		if errC := epc.Close(); errC != nil {
			t.Fatalf("error closing etcd processes (%v)", errC)
		}
	}()

	cx := ctlCtx{
		t:           t,
		cfg:         configNoTLS,
		dialTimeout: 7 * time.Second,
		quorum:      true,
		epc:         epc,
	}
	var kvs []kv
	for i := 0; i < 50; i++ {
		kvs = append(kvs, kv{key: fmt.Sprintf("foo%02d", i), val: "bar"})
		if err := ctlV3Put(cx, kvs[i].key, kvs[i].val, ""); err != nil {
			cx.t.Fatalf("#%d: ctlV3Put error (%v)", i, err)
		}
	}

	for i := range epc.procs {
		if err := epc.procs[i].Stop(); err != nil {
			t.Fatalf("#%d: error closing etcd process (%v)", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(epc.procs))
	for i := range epc.procs {
		go func(i int) {
			epc.procs[i].Config().execPath = binDir + "/etcd"
			epc.procs[i].Config().keepDataDir = true
			if err := epc.procs[i].Restart(); err != nil {
				t.Fatalf("error restarting etcd process (%v)", err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	if err := ctlV3Get(cx, []string{"--prefix", "foo"}, kvs...); err != nil {
		t.Fatal(err)
	}
}

// TestReleaseAbandonUpgrade checks that after partially upgrading a cluster
// restarting with the previous minor revision binary will work with the
// same data directory.
func TestReleaseAbandonUpgrade(t *testing.T) {
	defer testutil.AfterTest(t)

	cfg := configNoTLS
	cfg.execPath = lastReleaseEtcd(t)
	cfg.snapCount = 3
	cfg.baseScheme = "unix"

	epc, err := newEtcdProcessCluster(&cfg)
	testutil.AssertNil(t, err)
	defer func() { testutil.AssertNil(t, epc.Close()) }()

	cx := ctlCtx{
		t:           t,
		cfg:         configNoTLS,
		dialTimeout: 7 * time.Second,
		quorum:      true,
		epc:         epc,
	}
	kvs := make([]kv, 5)
	for i := range kvs {
		kvs[i] = kv{key: fmt.Sprintf("foo%d", i), val: "bar"}
		testutil.AssertNil(t, ctlV3Put(cx, kvs[i].key, kvs[i].val, ""))
	}

	// partially upgrade
	for _, proc := range epc.procs[1:] {
		testutil.AssertNil(t, proc.Stop())
		proc.Config().execPath = binDir + "/etcd"
		proc.Config().keepDataDir = true
		testutil.AssertNil(t, proc.Restart())
	}

	// downgrade
	for _, proc := range epc.procs[1:] {
		testutil.AssertNil(t, proc.Stop())
		proc.Config().execPath = cfg.execPath
		testutil.AssertNil(t, proc.Restart())
		err := ctlV3Get(cx, []string{"--prefix", "foo"}, kvs...)
		testutil.AssertNil(t, err)
	}
}

func lastReleaseEtcd(t *testing.T) string {
	lastReleaseBinary := binDir + "/etcd-last-release"
	if !fileutil.Exist(lastReleaseBinary) {
		t.Skipf("%q does not exist", lastReleaseBinary)
	}
	return lastReleaseBinary
}
