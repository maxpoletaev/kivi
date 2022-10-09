package clustering

import (
	"fmt"

	"github.com/go-kit/log/level"
)

// DispatchEvent executes a handler associated with a particular event.
func (c *Cluster) DispatchEvent(e ClusterEvent) error {
	switch event := e.(type) {
	case *NodeJoined:
		c.handleNodeJoined(event)
	case *NodeLeft:
		c.handleNodeLeft(event)
	case *NodeStatusChanged:
		c.handleStatusChanged(event)
	default:
		return fmt.Errorf("unknown event type: %T", event)
	}

	return nil
}

func (c *Cluster) handleNodeJoined(event *NodeJoined) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if _, ok := c.members[event.NodeID]; ok {
		level.Error(c.logger).Log(
			"msg", "failed to register new cluster member: duplicate member id",
			"id", event.NodeID,
			"name", event.NodeName,
			"addr", event.ServerAddr,
		)

		return
	}

	node := Member{
		ID:         event.NodeID,
		Name:       event.NodeName,
		GossipAddr: event.GossipAddr,
		ServerAddr: event.ServerAddr,
		Status:     StatusAlive,
	}

	c.members[node.ID] = node

	c.eventPub.RegisterNode(&node)

	conn, err := c.createConnection(&node)
	if err != nil {
		level.Error(c.logger).Log(
			"msg", "failed to connect to a member",
			"addr", node.ServerAddr,
			"err", err,
		)

		return
	}

	c.connections[node.ID] = conn

	level.Info(c.logger).Log(
		"msg", "new member joined",
		"id", node.ID,
		"name", node.Name,
		"addr", node.ServerAddr,
	)
}

func (c *Cluster) handleNodeLeft(event *NodeLeft) {
	c.mut.Lock()
	defer c.mut.Unlock()

	node, found := c.members[event.NodeID]
	if !found {
		return
	}

	if conn, ok := c.connections[event.NodeID]; ok {
		if err := conn.Close(); err != nil {
			level.Warn(c.logger).Log("msg", "failed to close connection to node", "err", err)
		}
	}

	c.eventPub.UnregisterNode(&node)

	delete(c.members, event.NodeID)

	delete(c.connections, event.NodeID)

	level.Info(c.logger).Log("msg", "member left", "id", event.NodeID)
}

func (c *Cluster) handleStatusChanged(event *NodeStatusChanged) {
	c.mut.Lock()
	defer c.mut.Unlock()

	node, found := c.members[event.NodeID]
	if !found {
		return
	}

	level.Info(c.logger).Log(
		"msg", "node status changed",
		"source.node_id", event.SourceID,
		"node_id", event.NodeID,
		"old_status", event.OldStatus,
		"new_status", event.Status,
	)

	if event.Version > node.Version {
		node.Version = event.Version
		node.Status = event.Status
		c.members[node.ID] = node
		return
	}
}
