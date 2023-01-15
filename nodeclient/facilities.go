package nodeclient

//go:generate mockgen -source=facilities.go -destination=facilities_mock.go -package=nodeclient

import (
	"github.com/maxpoletaev/kiwi/membership"
)

// MemberRegistry is used to manage the cluster members.
type MemberRegistry interface {
	SelfID() membership.NodeID
	Members() []membership.Member
	HasMember(membership.NodeID) bool
	Member(membership.NodeID) (membership.Member, bool)
	Add(members ...membership.Member) error
}
