package nodeclient

//go:generate mockgen -destination=facilities_mock.go -package=clust -source=facilities.go

import (
	"github.com/maxpoletaev/kv/membership"
)

// MemberRegistry is used to manage the cluster members.
type MemberRegistry interface {
	Members() []membership.Member
	HasMember(membership.NodeID) bool
	Member(membership.NodeID) (membership.Member, bool)
	Add(members ...membership.Member) error
}
