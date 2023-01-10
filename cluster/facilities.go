package cluster

//go:generate mockgen -destination=facilities_mock_test.go -package=cluster -source=facilities.go

import (
	"context"

	"github.com/maxpoletaev/kv/membership"
)

// MemberRegistry is used to manage the cluster members.
type MemberRegistry interface {
	Members() []membership.Member
	HasMember(membership.NodeID) bool
	Member(membership.NodeID) (membership.Member, bool)
	Add(members ...membership.Member) error
}

// Dialer is used to create new connections to the cluster members.
type Dialer interface {
	DialContext(ctx context.Context, addr string) (Conn, error)
}
