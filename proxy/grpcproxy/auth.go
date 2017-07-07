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
	resp, err := ap.a.AuthEnable(ctx)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthEnableResponse)(resp), nil
}

func (ap *authProxy) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	resp, err := ap.a.AuthDisable(ctx)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthDisableResponse)(resp), nil
}

func (ap *authProxy) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	return pb.NewAuthClient(ap.a.ActiveConnection()).Authenticate(ctx, r)
}

func (ap *authProxy) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := ap.a.RoleAdd(ctx, r.Name)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthRoleAddResponse)(resp), err
}

func (ap *authProxy) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := ap.a.RoleDelete(ctx, r.Role)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthRoleDeleteResponse)(resp), err
}

func (ap *authProxy) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := ap.a.RoleGet(ctx, r.Role)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthRoleGetResponse)(resp), nil
}

func (ap *authProxy) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := ap.a.RoleList(ctx)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthRoleListResponse)(resp), nil
}

func (ap *authProxy) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := ap.a.RoleRevokePermission(ctx, r.Role, r.Key, r.RangeEnd)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthRoleRevokePermissionResponse)(resp), nil
}

func (ap *authProxy) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	key, end, perm := string(r.Perm.Key), string(r.Perm.RangeEnd), clientv3.PermissionType(r.Perm.PermType)
	resp, err := ap.a.RoleGrantPermission(ctx, string(r.Name), key, end, perm)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthRoleGrantPermissionResponse)(resp), nil
}

func (ap *authProxy) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	resp, err := ap.a.UserAdd(ctx, string(r.Name), string(r.Password))
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserAddResponse)(resp), nil
}

func (ap *authProxy) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := ap.a.UserDelete(ctx, string(r.Name))
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserDeleteResponse)(resp), nil
}

func (ap *authProxy) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := ap.a.UserGet(ctx, string(r.Name))
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserGetResponse)(resp), nil
}

func (ap *authProxy) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := ap.a.UserList(ctx)
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserListResponse)(resp), nil
}

func (ap *authProxy) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := ap.a.UserGrantRole(ctx, string(r.User), string(r.Role))
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserGrantRoleResponse)(resp), nil
}

func (ap *authProxy) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := ap.a.UserRevokeRole(ctx, string(r.Name), string(r.Role))
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserRevokeRoleResponse)(resp), nil
}

func (ap *authProxy) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := ap.a.UserChangePassword(ctx, string(r.Name), string(r.Password))
	if err != nil {
		return nil, err
	}
	return (*pb.AuthUserChangePasswordResponse)(resp), nil
}
