package network

import (
	"github.com/go-kit/log/level"
)

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

	node := Node{
		ID:         event.NodeID,
		Name:       event.NodeName,
		GossipAddr: event.GossipAddr,
		ServerAddr: event.ServerAddr,
	}

	c.members[node.ID] = node

	c.eventPub.Subscribe(node.ID, node.GossipAddr)

	conn, err := c.connectNode(&node)
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

	if conn, ok := c.connections[event.NodeID]; ok {
		if err := conn.Close(); err != nil {
			level.Warn(c.logger).Log("msg", "failed to close connection to node", "err", err)
		}
	}

	delete(c.members, event.NodeID)

	delete(c.connections, event.NodeID)

	c.eventPub.Unsubscribe(event.NodeID)

	level.Info(c.logger).Log("msg", "member left", "id", event.NodeID)
}
