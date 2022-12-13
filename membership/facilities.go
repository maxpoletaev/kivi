package membership

//go:generate mockgen -source=facilities.go -destination=facilities_mock_test.go -package=membership

// EventPub is an interface for publishing cluster events.
type EventPub interface {
	Broadcast(ClusterEvent) error
	RegisterReceiver(*Member) error
	UnregisterReceiver(*Member)
}
