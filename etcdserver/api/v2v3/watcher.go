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

package v2v3

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	etcdErr "github.com/coreos/etcd/error"
	"github.com/coreos/etcd/store"
)

func (s *v2v3Store) Watch(prefix string, recursive, stream bool, sinceIndex uint64) (store.Watcher, error) {
	if recursive {
		// TODO: watch all keys and filter
		return nil, fmt.Errorf("recursive unsupported")
	}
	ctx, cancel := context.WithCancel(s.ctx)
	wch := s.c.Watch(
		ctx,
		mkPath(prefix),
		clientv3.WithPrefix(),
		clientv3.WithRev(mkV3Rev(sinceIndex)),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV())

	resp, ok := <-wch
	if err := resp.Err(); err != nil || !ok {
		return nil, etcdErr.NewError(etcdErr.EcodeRaftInternal, prefix, 0)
	}

	evc, donec := make(chan *store.Event), make(chan struct{})
	go func() {
		defer func() {
			close(evc)
			close(donec)
		}()
		for resp := range wch {
			for _, ev := range resp.Events {
				evc <- mkV2Event(resp, ev)
				if !stream {
					return
				}
			}
		}
	}()

	return &v2v3Watcher{
		startRev: resp.Header.Revision,
		evc:      evc,
		donec:    donec,
		cancel:   cancel,
	}, nil
}

func mkV2Event(wr clientv3.WatchResponse, ev *clientv3.Event) *store.Event {
	act := ""
	// TODO: more accurate action tracking / fewer actions in interface
	switch {
	case ev.IsCreate():
		act = store.Create
	case ev.IsModify():
		act = store.Update
	case ev.Type == clientv3.EventTypeDelete:
		act = store.Delete
	}
	return &store.Event{
		Action:    act,
		Node:      mkV2Node(ev.Kv),
		PrevNode:  mkV2Node(ev.PrevKv),
		EtcdIndex: mkV2Rev(wr.Header.Revision),
	}
}

type v2v3Watcher struct {
	startRev int64
	evc      chan *store.Event
	donec    chan struct{}
	cancel   context.CancelFunc
}

func (w *v2v3Watcher) StartIndex() uint64 { return mkV2Rev(w.startRev) }

func (w *v2v3Watcher) Remove() {
	w.cancel()
	<-w.donec
}

func (w *v2v3Watcher) EventChan() chan *store.Event { return w.evc }
