package membership

//go:generate mockgen -source=facilities.go -destination=facilities_mock_test.go -package=membership

// EventSender is an interface for publishing cluster events.
type EventSender interface {
	Broadcast(ClusterEvent) error
	RegisterReceiver(*Member) error
	UnregisterReceiver(*Member)
}
