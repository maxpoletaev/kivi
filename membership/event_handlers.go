package membership

import (
	"fmt"

	"github.com/go-kit/log/level"
)

func (c *Memberlist) handleMemberJoined(event *MemberJoined) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if _, ok := c.members[event.ID]; ok {
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

	if err := c.eventBus.RegisterReceiver(&member); err != nil {
		return fmt.Errorf("failed to register member in gossiper: %v", err)
	}

	c.members[event.ID] = member

	level.Debug(c.logger).Log("msg", "member joined", "name", event.Name)

	return nil
}

func (c *Memberlist) handleMemberLeft(event *MemberLeft) (err error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	member, ok := c.members[event.ID]
	if !ok {
		return
	}

	// Node is expeled from the cluster by another node.
	if member.ID == c.selfID {
		for _, member := range c.members {
			if member.ID != c.selfID {
				c.eventBus.UnregisterReceiver(&member)
			}
		}

		// Reset the list of members to only contain the local node.
		c.members = make(map[NodeID]Member, 1)
		c.members[member.ID] = member
	}

	delete(c.members, event.ID)
	c.eventBus.UnregisterReceiver(&member)
	level.Debug(c.logger).Log("msg", "member left", "name", member.Name)

	return
}

func (c *Memberlist) handleMemberUpdated(event *MemberUpdated) (err error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	member, ok := c.members[event.ID]
	if !ok {
		return
	}

	if member.Version > event.Version {
		return
	}

	oldStatus := member.Status
	member.Status = event.Status
	member.Version = event.Version
	c.members[event.ID] = member

	if oldStatus != event.Status {
		level.Debug(c.logger).Log(
			"msg", "member status changed",
			"name", member.Name,
			"old_status", oldStatus,
			"new_status", event.Status,
		)
	}

	return nil
}
