#  etcd roadmap

**work in progress**

This document defines a high level roadmap for etcd development.

The dates below should not be considered authoritative, but rather indicative of the projected timeline of the project. The [milestones defined in GitHub](https://github.com/coreos/etcd/milestones) represent the most up-to-date and issue-for-issue plans.

etcd 3.2 the current stable etcd branch. The roadmap below outlines new features that will be added to etcd, and while subject to change, define what future stable will look like.

## etcd 3.3 (2017-Oct)

- Transaction nesting
- Transaction range comparisons
- Move leader
- No free list write-out in backend
- Cluster version rollback
- Client failover on cluster partition
- Client failover on mute watches
- Alpha online integrity checking
- Alpha v2 emulation
- Alpha linearized read leasing proxy

##  etcd 3.4 (2018-April)

- RPC RBAC
- Stable v2 emulation over v3
- Proxy-as-client interface passthrough
- Mirroring proxy
- Configurable proxy services
- Multi-reader linearized read leasing client
- Alpha external virtualized auth
