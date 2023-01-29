package membership

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/internal/multierror"
)

var ErrNoSuchMember = errors.New("member not found")

type Memberlist struct {
	mut         sync.RWMutex
	selfID      NodeID
	members     map[NodeID]Member
	eventSender EventSender
	logger      log.Logger
	lastUpdate  time.Time
}

func New(self Member, logger log.Logger, es EventSender) *Memberlist {
	ml := &Memberlist{
		selfID:      self.ID,
		logger:      logger,
		eventSender: es,
		members:     make(map[NodeID]Member, 1),
	}

	self.Status = StatusHealthy
	ml.members[self.ID] = self

	return ml
}

func (ml *Memberlist) LastUpdate() time.Time {
	ml.mut.RLock()
	defer ml.mut.RUnlock()

	return ml.lastUpdate
}

// ConsumeEvents consumes events from the event channel and updates the cluster state.
func (ml *Memberlist) ConsumeEvents(ch <-chan ClusterEvent) {
	go func() {
		for event := range ch {
			if err := ml.handleEvent(event); err != nil {
				level.Error(ml.logger).Log("msg", "failed to handle event", "err", err)
			}
		}
	}()
}

// Members returns a list of known cluster members.
func (ml *Memberlist) Members() []Member {
	ml.mut.RLock()
	defer ml.mut.RUnlock()

	return generic.MapValues(ml.members)
}

func (ml *Memberlist) HasMember(id NodeID) bool {
	ml.mut.RLock()
	defer ml.mut.RUnlock()

	_, ok := ml.members[id]

	return ok
}

func (ml *Memberlist) Member(id NodeID) (Member, bool) {
	ml.mut.RLock()
	defer ml.mut.RUnlock()

	member, found := ml.members[id]
	if !found {
		return Member{}, false
	}

	return member, true
}

func (ml *Memberlist) Self() Member {
	ml.mut.RLock()
	defer ml.mut.RUnlock()

	return ml.members[ml.selfID]
}

func (ml *Memberlist) SelfID() NodeID {
	return ml.selfID
}

// Add adds new members to the cluster. It is expected that if there are multiple members on
// the list, they already form a cluster and already know about each other. Adding several
// independent nodes requires calling this method once per each node.
func (ml *Memberlist) Add(members ...Member) error {
	ml.mut.Lock()
	defer ml.mut.Unlock()

	errs := multierror.New[NodeID]()
	newMembers := make([]Member, 0)

	// Filter out the ones that we already know about.
	for _, n := range members {
		if _, found := ml.members[n.ID]; !found {
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

		if err := ml.eventSender.Broadcast(event); err != nil {
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

		if err := ml.eventSender.Register(m); err != nil {
			errs.Add(m.ID, fmt.Errorf("failed to register receiver: %w", err))
			continue
		}

		ml.members[m.ID] = *m
	}

	ml.lastUpdate = time.Now()

	return errs.Combined()
}

func (ml *Memberlist) Expel(id NodeID) error {
	ml.mut.Lock()
	defer ml.mut.Unlock()

	event := &MemberLeft{
		ID:       id,
		SourceID: ml.selfID,
	}

	err := ml.eventSender.Broadcast(event)
	if err != nil {
		return err
	}

	member, found := ml.members[id]
	if !found {
		return ErrNoSuchMember
	}

	ml.lastUpdate = time.Now()

	ml.eventSender.Unregister(&member)

	delete(ml.members, id)

	return nil
}

func (ml *Memberlist) Leave() error {
	event := &MemberLeft{
		ID:       ml.selfID,
		SourceID: ml.selfID,
	}

	// Notify other cluster members about the node leaving the cluster.
	err := ml.eventSender.Broadcast(event)
	if err != nil {
		return err
	}

	ml.mut.Lock()
	defer ml.mut.Unlock()

	self := ml.members[ml.selfID]

	// Unregister remote members from the gossiper.
	for _, member := range ml.members {
		if member.ID != ml.selfID {
			ml.eventSender.Unregister(&member)
		}
	}

	// Reset the list of members to only contain the local node.
	ml.members = make(map[NodeID]Member, 1)
	ml.members[self.ID] = self

	ml.lastUpdate = time.Now()

	return nil
}

// SetStatus changes member status across all other members of the cluster.
func (ml *Memberlist) SetStatus(id NodeID, status Status) (Status, error) {
	ml.mut.Lock()
	defer ml.mut.Unlock()

	member, ok := ml.members[id]
	if !ok {
		return 0, ErrNoSuchMember
	}

	if member.Status == status {
		return member.Status, nil
	}

	oldStatus := member.Status

	member.Status = status

	member.Version++

	err := ml.eventSender.Broadcast(&MemberUpdated{
		SourceID: ml.selfID,
		ID:       member.ID,
		Version:  member.Version,
		Status:   status,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to publish NodeStatusChanged event: %w", err)
	}

	ml.members[id] = member

	ml.lastUpdate = time.Now()

	return oldStatus, nil
}

func (ml *Memberlist) handleEvent(e ClusterEvent) error {
	var err error

	switch event := e.(type) {
	case *MemberJoined:
		err = ml.handleMemberJoined(event)
	case *MemberLeft:
		err = ml.handleMemberLeft(event)
	case *MemberUpdated:
		err = ml.handleMemberUpdated(event)
	default:
		err = fmt.Errorf("unknown event type: %T", event)
	}

	if err != nil {
		ml.mut.Lock()
		ml.lastUpdate = time.Now()
		ml.mut.Unlock()
	}

	return err
}
