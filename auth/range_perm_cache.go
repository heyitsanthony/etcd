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

package auth

import (
	"sync"

	"github.com/coreos/etcd/auth/authpb"
	"github.com/coreos/etcd/pkg/adt"
)

type unifiedRangePermissions struct {
	readPerms  *adt.IntervalTree
	writePerms *adt.IntervalTree
}

func getMergedRolePerms(roles []*authpb.Role) *unifiedRangePermissions {
	readPerms, writePerms := &adt.IntervalTree{}, &adt.IntervalTree{}
	for _, role := range roles {
		for _, perm := range role.KeyPermission {
			var ivl adt.Interval
			var rangeEnd []byte

			if len(perm.RangeEnd) != 1 || perm.RangeEnd[0] != 0 {
				rangeEnd = perm.RangeEnd
			}

			if len(perm.RangeEnd) != 0 {
				ivl = adt.NewBytesAffineInterval(perm.Key, rangeEnd)
			} else {
				ivl = adt.NewBytesAffinePoint(perm.Key)
			}

			switch perm.PermType {
			case authpb.READWRITE:
				readPerms.Insert(ivl, struct{}{})
				writePerms.Insert(ivl, struct{}{})
			case authpb.READ:
				readPerms.Insert(ivl, struct{}{})
			case authpb.WRITE:
				writePerms.Insert(ivl, struct{}{})
			}
		}
	}
	return &unifiedRangePermissions{
		readPerms:  readPerms,
		writePerms: writePerms,
	}
}

func checkKeyInterval(cachedPerms *unifiedRangePermissions, key, rangeEnd []byte, permtyp authpb.Permission_Type) bool {
	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
		rangeEnd = nil
	}

	ivl := adt.NewBytesAffineInterval(key, rangeEnd)
	switch permtyp {
	case authpb.READ:
		return cachedPerms.readPerms.Contains(ivl)
	case authpb.WRITE:
		return cachedPerms.writePerms.Contains(ivl)
	default:
		plog.Panicf("unknown auth type: %v", permtyp)
	}
	return false
}

func checkKeyPoint(cachedPerms *unifiedRangePermissions, key []byte, permtyp authpb.Permission_Type) bool {
	pt := adt.NewBytesAffinePoint(key)
	switch permtyp {
	case authpb.READ:
		return cachedPerms.readPerms.Intersects(pt)
	case authpb.WRITE:
		return cachedPerms.writePerms.Intersects(pt)
	default:
		plog.Panicf("unknown auth type: %v", permtyp)
	}
	return false
}

type rangeChecker struct {
	mu        sync.RWMutex
	permCache map[string]*unifiedRangePermissions // keyed by username
}

func NewRangeChecker() *rangeChecker {
	return &rangeChecker{
		permCache: make(map[string]*unifiedRangePermissions),
	}
}

func (rc *rangeChecker) Clear() {
	rc.permCache = make(map[string]*unifiedRangePermissions)
}

func (rc *rangeChecker) Invalidate(user string) {
	delete(rc.permCache, user)
}

func (rc *rangeChecker) IsPermitted(ar AuthReader, userName string, key, rangeEnd []byte, permtyp authpb.Permission_Type) bool {
	rc.mu.RLock()
	perms, ok := rc.permCache[userName]
	rc.mu.RUnlock()
	if !ok {
		user := ar.User(userName)
		if user == nil {
			plog.Errorf("failed unifying permissions for invalid user %s", userName)
			return false
		}
		var roles []*authpb.Role
		for _, roleName := range user.Roles {
			if role := ar.Role(roleName); role != nil {
				roles = append(roles, role)
			}
		}
		perms = getMergedRolePerms(roles)
		rc.mu.Lock()
		rc.permCache[userName] = perms
		rc.mu.Unlock()
	}

	if len(rangeEnd) == 0 {
		return checkKeyPoint(perms, key, permtyp)
	}
	return checkKeyInterval(perms, key, rangeEnd, permtyp)
}
