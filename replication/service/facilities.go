package service

//go:generate mockgen -source=facilities.go -destination=facilities_mock_test.go -package=service

import (
	"github.com/maxpoletaev/kiwi/membership"
)

type Memberlist interface {
	SelfID() membership.NodeID
	Members() []membership.Member
}
