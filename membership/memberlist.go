package membership

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-kit/log"

	"github.com/maxpoletaev/kv/internal/generic"
	"github.com/maxpoletaev/kv/internal/multierror"
)

var (
	ErrMemberNotFound = errors.New("member not found")
)

type Memberlist struct {
	mut      sync.RWMutex
	selfID   NodeID
	members  map[NodeID]Member
	eventBus EventSender
	logger   log.Logger
}

func New(self Member, logger log.Logger, eb EventSender) *Memberlist {
	ml := &Memberlist{
		selfID:   self.ID,
		logger:   logger,
		eventBus: eb,
		members:  make(map[NodeID]Member, 1),
	}

	self.Status = StatusHealthy
	ml.members[self.ID] = self

	return ml
}

func (c *Memberlist) ConsumeEvents(ch <-chan ClusterEvent) {
	go func() {
		for event := range ch {
			c.handleEvent(event)
		}
	}()
}

// Members returns a list of known cluster members.
func (c *Memberlist) Members() []Member {
	c.mut.RLock()
	defer c.mut.RUnlock()

	return generic.MapValues(c.members)
}

func (c *Memberlist) HasMember(id NodeID) bool {
	c.mut.RLock()
	defer c.mut.RUnlock()

	_, ok := c.members[id]

	return ok
}

func (c *Memberlist) Member(id NodeID) (Member, bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	member, found := c.members[id]
	if !found {
		return Member{}, false
	}

	return member, true
}

func (c *Memberlist) Self() Member {
	c.mut.RLock()
	defer c.mut.RUnlock()

	return c.members[c.selfID]
}

// Add adds new members to the cluster. It is expected that if there are multiple members on
// the list, they already form a cluster and already know about each other. Adding several
// independent nodes requires calling this method once per each node.
func (c *Memberlist) Add(members ...Member) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	errs := multierror.New[NodeID]()

	// Filter out the ones that we already know about.
	newMembers := make([]Member, 0)
	for _, n := range members {
		if _, found := c.members[n.ID]; !found {
			newMembers = append(newMembers, n)
		}
	}

	// Share the info about new members among the already known members.
	// This is important to do before the new members are registered in the gossiper.
	for i := range newMembers {
		m := &newMembers[i]

		event := &MemberJoined{
			ID:         m.ID,
			Name:       m.Name,
			ServerAddr: m.ServerAddr,
			GossipAddr: m.GossipAddr,
		}

		// TODO: So far we do not care much if the broadcast fails since we also
		//   rely on periodic state synchronization (not implemented yet).

		if err := c.eventBus.Broadcast(event); err != nil {
			errs.Add(m.ID, fmt.Errorf("failed to broadcast: %w", err))
			continue
		}
	}

	// Subscribe new members to the broadcast messages.
	for i := range newMembers {
		m := &newMembers[i]

		if _, hasError := errs.Get(m.ID); hasError {
			continue
		}

		if err := c.eventBus.RegisterReceiver(m); err != nil {
			errs.Add(m.ID, fmt.Errorf("failed to register receiver: %w", err))
			continue
		}

		c.members[m.ID] = *m
	}

	return errs.Ret()
}

func (c *Memberlist) Expel(id NodeID) error {
	event := &MemberLeft{
		ID:       id,
		SourceID: c.selfID,
	}

	member, found := c.members[id]
	if !found {
		return ErrMemberNotFound
	}

	err := c.eventBus.Broadcast(event)
	if err != nil {
		return err
	}

	c.eventBus.UnregisterReceiver(&member)

	delete(c.members, id)

	return nil
}

// RemoveMember removes the given member from the cluster. All other nodes will eventually
// recevie a message about a node leaving the cluster, but this will not happen
// immediately. Until then the node may still receive the requests from other nodes.
func (c *Memberlist) Leave() error {
	event := &MemberLeft{
		ID:       c.selfID,
		SourceID: c.selfID,
	}

	// Notify other cluster members about the node leaving the cluster.
	err := c.eventBus.Broadcast(event)
	if err != nil {
		return err
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	self := c.members[c.selfID]

	// Unregister remote members from the gossiper.
	for _, member := range c.members {
		if member.ID != c.selfID {
			c.eventBus.UnregisterReceiver(&member)
		}
	}

	// Reset the list of members to only contain the local node.
	c.members = make(map[NodeID]Member, 1)
	c.members[self.ID] = self

	return nil
}

// SetStatus changes member status across all other members of the cluster.
func (c *Memberlist) SetStatus(id NodeID, status Status) (Status, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	member, ok := c.members[id]
	if !ok {
		return 0, ErrMemberNotFound
	}

	if member.Status == status {
		return member.Status, nil
	}

	oldStatus := member.Status

	member.Status = status

	member.Version++

	err := c.eventBus.Broadcast(&MemberUpdated{
		SourceID: c.selfID,
		ID:       member.ID,
		Version:  member.Version,
		Status:   status,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to publish NodeStatusChanged event: %w", err)
	}

	c.members[id] = member

	return oldStatus, nil
}

func (c *Memberlist) handleEvent(e ClusterEvent) error {
	var err error

	switch event := e.(type) {
	case *MemberJoined:
		err = c.handleMemberJoined(event)
	case *MemberLeft:
		err = c.handleMemberLeft(event)
	case *MemberUpdated:
		err = c.handleMemberUpdated(event)
	default:
		err = fmt.Errorf("unknown event type: %T", event)
	}

	return err
}
