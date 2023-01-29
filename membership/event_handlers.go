package membership

import (
	"fmt"

	"github.com/go-kit/log/level"
)

func (ml *Memberlist) handleMemberJoined(event *MemberJoined) error {
	ml.mut.Lock()
	defer ml.mut.Unlock()

	if _, ok := ml.members[event.ID]; ok {
		return nil
	}

	member := Member{
		ID:         event.ID,
		Name:       event.Name,
		GossipAddr: event.GossipAddr,
		ServerAddr: event.ServerAddr,
		Status:     StatusHealthy,
		Version:    1,
	}

	if err := ml.eventSender.Register(&member); err != nil {
		return fmt.Errorf("failed to register member in gossiper: %v", err)
	}

	ml.members[event.ID] = member

	level.Debug(ml.logger).Log("msg", "member joined", "name", event.Name)

	return nil
}

func (ml *Memberlist) handleMemberLeft(event *MemberLeft) (err error) {
	ml.mut.Lock()
	defer ml.mut.Unlock()

	member, ok := ml.members[event.ID]
	if !ok {
		return
	}

	// Node is expelled from the cluster by another node.
	if member.ID == ml.selfID {
		for _, member := range ml.members {
			if member.ID != ml.selfID {
				ml.eventSender.Unregister(&member)
			}
		}

		// Reset the list of members to only contain the local node.
		ml.members = make(map[NodeID]Member, 1)
		ml.members[member.ID] = member
	}

	delete(ml.members, event.ID)
	ml.eventSender.Unregister(&member)
	level.Debug(ml.logger).Log("msg", "member left", "name", member.Name)

	return
}

func (ml *Memberlist) handleMemberUpdated(event *MemberUpdated) (err error) {
	ml.mut.Lock()
	defer ml.mut.Unlock()

	member, ok := ml.members[event.ID]
	if !ok {
		return
	}

	if member.Version > event.Version {
		return
	}

	oldStatus := member.Status
	member.Status = event.Status
	member.Version = event.Version
	ml.members[event.ID] = member

	if oldStatus != event.Status {
		level.Debug(ml.logger).Log(
			"msg", "member status changed",
			"name", member.Name,
			"old_status", oldStatus,
			"new_status", event.Status,
		)
	}

	return nil
}
