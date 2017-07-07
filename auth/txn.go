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

package auth

import (
	"encoding/binary"

	"github.com/coreos/etcd/auth/authpb"
	"github.com/coreos/etcd/mvcc/backend"
)

type AuthReader interface {
	User(username string) *authpb.User
	Role(role string) *authpb.Role
	Users() []*authpb.User
	Roles() []*authpb.Role
}

// authTxn wraps a txn with methods to access auth store buckets.
// It implements an AuthReader.
type authTxn struct {
	tx backend.BatchTx
}

func newAuthTxn(be backend.Backend) *authTxn {
	at := &authTxn{tx: be.BatchTx()}
	at.tx.Lock()
	return at
}

func (at *authTxn) Close() {
	at.tx.Unlock()
	at.tx = nil
}

func (at *authTxn) User(username string) *authpb.User {
	_, vs := at.tx.UnsafeRange(authUsersBucketName, []byte(username), nil, 0)
	if len(vs) == 0 {
		return nil
	}
	user := &authpb.User{}
	if err := user.Unmarshal(vs[0]); err != nil {
		plog.Panicf("failed to unmarshal user struct (name: %s): %s", username, err)
	}
	return user
}

func (at *authTxn) Role(rolename string) *authpb.Role {
	_, vs := at.tx.UnsafeRange(authRolesBucketName, []byte(rolename), nil, 0)
	if len(vs) == 0 {
		return nil
	}
	role := &authpb.Role{}
	if err := role.Unmarshal(vs[0]); err != nil {
		plog.Panicf("failed to unmarshal role struct (name: %s): %s", rolename, err)
	}
	return role
}

func (at *authTxn) PutUser(user *authpb.User) {
	b, err := user.Marshal()
	if err != nil {
		plog.Panicf("failed to marshal user struct (name: %s): %s", user.Name, err)
	}
	at.tx.UnsafePut(authUsersBucketName, user.Name, b)
}

func (at *authTxn) DelUser(username string) {
	at.tx.UnsafeDelete(authUsersBucketName, []byte(username))
}

func (at *authTxn) PutRole(role *authpb.Role) {
	b, err := role.Marshal()
	if err != nil {
		plog.Panicf("failed to marshal role struct (name: %s): %s", role.Name, err)
	}
	at.tx.UnsafePut(authRolesBucketName, []byte(role.Name), b)
}

func (at *authTxn) DelRole(rolename string) {
	at.tx.UnsafeDelete(authRolesBucketName, []byte(rolename))
}

func (at *authTxn) Roles() []*authpb.Role {
	_, vs := at.tx.UnsafeRange(authRolesBucketName, []byte{0}, []byte{0xff}, -1)
	if len(vs) == 0 {
		return nil
	}

	var roles []*authpb.Role
	for _, v := range vs {
		role := &authpb.Role{}
		if err := role.Unmarshal(v); err != nil {
			plog.Panicf("failed to unmarshal role struct: %s", err)
		}
		roles = append(roles, role)
	}
	return roles
}

func (at *authTxn) Users() []*authpb.User {
	_, vs := at.tx.UnsafeRange(authUsersBucketName, []byte{0}, []byte{0xff}, -1)
	if len(vs) == 0 {
		return nil
	}

	var users []*authpb.User
	for _, v := range vs {
		user := &authpb.User{}
		if err := user.Unmarshal(v); err != nil {
			plog.Panicf("failed to unmarshal user struct: %s", err)
		}
		users = append(users, user)
	}
	return users
}

func (at *authTxn) Revision() uint64 {
	_, vs := at.tx.UnsafeRange(authBucketName, []byte(revisionKey), nil, 0)
	if len(vs) != 1 {
		// this can happen in the initialization phase
		return 0
	}
	return binary.BigEndian.Uint64(vs[0])
}
