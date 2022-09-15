package network

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kv/network/proto"
)

type eventPublisher interface {
	Publish(ClusterEvent) error
	Subscribe(NodeID, string)
	Unsubscribe(NodeID)
}

type ConnectedNode struct {
	*Connection
	Node
}

type Cluster struct {
	localNode      Node
	mut            sync.RWMutex
	members        map[NodeID]Node
	connections    map[NodeID]*Connection
	connectTimeout time.Duration
	eventPub       eventPublisher
	logger         log.Logger
}

func NewCluster(localNode Node, logger log.Logger, eventPub eventPublisher) *Cluster {
	members := make(map[NodeID]Node)
	members[localNode.ID] = localNode

	return &Cluster{
		localNode:      localNode,
		logger:         logger,
		members:        members,
		eventPub:       eventPub,
		connections:    make(map[NodeID]*Connection),
		connectTimeout: 5 * time.Second,
	}
}

func (c *Cluster) Nodes() []ConnectedNode {
	c.mut.RLock()
	defer c.mut.RUnlock()

	nodes := make([]ConnectedNode, 0)

	for id, node := range c.members {
		if conn, ok := c.connections[id]; ok {
			nodes = append(nodes, ConnectedNode{
				Connection: conn,
				Node:       node,
			})
		}
	}

	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	return nodes
}

func (c *Cluster) DispatchEvent(e ClusterEvent) error {
	switch event := e.(type) {
	case *NodeJoined:
		c.handleNodeJoined(event)
	case *NodeLeft:
		c.handleNodeLeft(event)
	default:
		return fmt.Errorf("unknown event type: %T", event)
	}

	return nil
}

func (c *Cluster) connectNode(node *Node) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.connectTimeout)
	defer cancel()

	serverAddr := node.ServerAddr
	if len(node.LocalServerAddr) != 0 {
		serverAddr = node.LocalServerAddr
	}

	conn, err := connect(ctx, serverAddr)
	if err != nil {
		return nil, fmt.Errorf("connection to %s has failed: %w", serverAddr, err)
	}

	level.Info(c.logger).Log("msg", "connected", "addr", serverAddr)

	return conn, nil
}

func (c *Cluster) Connect() (int, int, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	var lastErr error

	var connected, failed int

	for id, node := range c.members {
		if _, found := c.connections[id]; !found {
			conn, err := c.connectNode(&node)
			if err != nil {
				lastErr = err
				failed++
				continue
			}

			c.connections[id] = conn
			connected++
		}
	}

	return connected, failed, lastErr
}

func (c *Cluster) AddMembers(nodes []Node) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	nodesToAdd := make([]Node, 0)
	for _, n := range nodes {
		if _, found := c.members[n.ID]; !found {
			nodesToAdd = append(nodesToAdd, n)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))

	for _, n := range nodesToAdd {
		go func(n Node) {
			defer wg.Done()

			event := &NodeJoined{
				NodeID:     n.ID,
				NodeName:   n.Name,
				ServerAddr: n.ServerAddr,
				GossipAddr: n.GossipAddr,
			}

			if err := c.eventPub.Publish(event); err != nil {
				level.Error(c.logger).Log("msg", "failed to broadcast an event", "err", err)
			}
		}(n)
	}

	wg.Wait()

	for _, node := range nodesToAdd {
		c.eventPub.Subscribe(node.ID, node.GossipAddr)
		c.members[node.ID] = node
	}

	return nil
}

func (c *Cluster) JoinTo(joinAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := connect(ctx, joinAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to remote grpc server: %w", err)
	}

	resp, err := conn.Cluster.Join(ctx, &proto.JoinRequest{
		LocalNodes: func() (nodes []*proto.Node) {
			for _, node := range c.members {
				nodes = append(nodes, ToProtoNode(&node))
			}

			return
		}(),
	})
	if err != nil {
		return fmt.Errorf("failed ot join the cluster: %w", err)
	}

	remoteNodes := make([]Node, 0)
	for _, rn := range resp.RemoteNodes {
		remoteNodes = append(remoteNodes, FromProtoNode(rn))
	}

	c.AddMembers(remoteNodes)

	return nil
}

func (c *Cluster) Leave() error {
	c.mut.Lock()
	defer c.mut.Unlock()

	event := &NodeLeft{
		NodeID: c.localNode.ID,
	}

	err := c.eventPub.Publish(event)
	if err != nil {
		return err
	}

	for _, conn := range c.connections {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("failed to close grpc connection: %w", err)
		}
	}

	c.members = make(map[NodeID]Node)

	return nil
}
