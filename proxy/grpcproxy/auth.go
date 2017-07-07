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
	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type authProxy struct {
	a *clientv3.Client // clientv3.Auth doesn't export Authenticate
}

// NewAuthProxy creates an auth proxy that forwards incoming auth requests
// through the client's Auth interface.
func NewAuthProxy(client *clientv3.Client) pb.AuthServer {
	return &authProxy{a: client}
}

func (ap *authProxy) AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error) {
	return ap.a.AuthEnable(ctx)
}

func (ap *authProxy) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	return ap.a.AuthDisable(ctx)
}

func (ap *authProxy) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	return pb.NewAuthClient(ap.a.ActiveConnection()).Authenticate(ctx, r)
}

func (ap *authProxy) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	return ap.a.RoleAdd(ctx, r)
}

func (ap *authProxy) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	return ap.a.RoleDelete(ctx, r)
}

func (ap *authProxy) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	return ap.a.RoleGet(ctx, r)
}

func (ap *authProxy) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	return ap.a.RoleList(ctx, r)
}

func (ap *authProxy) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	return ap.a.RoleRevokePermission(ctx, r)
}

func (ap *authProxy) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	return ap.a.RoleGrantPermission(ctx, r)
}

func (ap *authProxy) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	return ap.a.UserAdd(ctx, r)
}

func (ap *authProxy) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	return ap.a.UserDelete(ctx, r)
}

func (ap *authProxy) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	return ap.a.UserGet(ctx, r)
}

func (ap *authProxy) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	return ap.a.UserList(ctx, r)
}

func (ap *authProxy) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	return ap.a.UserGrantRole(ctx, r)
}

func (ap *authProxy) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	return ap.a.UserRevokeRole(ctx, r)
}

func (ap *authProxy) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	return ap.a.UserChangePassword(ctx, r)
}
