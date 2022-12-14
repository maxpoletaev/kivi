package service

//go:generate mockgen -source=facilities.go -destination=facilities_mock.go -package service

import "github.com/maxpoletaev/kv/membership"

type MemberRegistry interface {
	Members() []membership.Member
	Add(...membership.Member) error
	Expel(membership.NodeID) error
}
