package clustering

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kv/clustering/proto"
)

type eventPublisher interface {
	Publish(ClusterEvent) error
	RegisterNode(*Member)
	UnregisterNode(*Member)
}

type Cluster struct {
	localID        NodeID
	mut            sync.RWMutex
	members        map[NodeID]Member
	connections    map[NodeID]*Connection
	connectHook    func(ctx context.Context, addr string) (*Connection, error)
	connectTimeout time.Duration
	eventPub       eventPublisher
	logger         log.Logger
}

// NewCluster creates new cluster. A cluster is always initialized with a single member
// representing the local host. Adding more members basically means merging several
// clusters together.
func NewCluster(localMember Member, logger log.Logger, eventPub eventPublisher) *Cluster {
	members := make(map[NodeID]Member)

	localMember.Status = StatusAlive

	members[localMember.ID] = localMember

	return &Cluster{
		logger:      logger,
		eventPub:    eventPub,
		connectHook: connect,

		members:        members,
		localID:        localMember.ID,
		connectTimeout: 5 * time.Second,
		connections:    make(map[NodeID]*Connection),
	}
}

// Nodes returns a list of connected nodes. Note that connected does not mean
// that the node is alive. It is up to the user to check the node status.
func (c *Cluster) Nodes() []Node {
	c.mut.RLock()
	defer c.mut.RUnlock()

	nodes := make([]Node, 0)

	for id, node := range c.members {
		local := node.ID == c.localID

		if conn, ok := c.connections[id]; ok {
			nodes = append(nodes, Node{
				Member:     node,
				Connection: conn,
				Local:      local,
			})
		}
	}

	return nodes
}

// NodeByID returns a node by id. Nil is returned if there is no such node.
func (c *Cluster) NodeByID(id NodeID) *Node {
	c.mut.RLock()
	defer c.mut.RUnlock()

	node, found := c.members[id]
	conn := c.connections[id]

	if found && conn != nil {
		local := node.ID == c.localID

		return &Node{
			Member:     node,
			Connection: conn,
			Local:      local,
		}
	}

	return nil
}

// LocalNode returns local node. Since local node cannot be removed from the cluster,
// this method should never return nil.
func (c *Cluster) LocalNode() *Node {
	return c.NodeByID(c.localID)
}

// NRandomNodes returns up to n random nodes according to the filter function.
func (c *Cluster) NRandomNodes(n int, filter func(node *Node) bool) []Node {
	nodes := make([]Node, 0, n)

	for _, node := range c.Nodes() {
		if filter != nil && filter(&node) {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) > 1 {
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
	}

	return nodes
}

func (c *Cluster) createConnection(member *Member) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.connectTimeout)
	defer cancel()

	serverAddr := member.ServerAddr
	if len(member.LocalServerAddr) != 0 {
		serverAddr = member.LocalServerAddr
	}

	conn, err := c.connectHook(ctx, serverAddr)
	if err != nil {
		return nil, fmt.Errorf("connection to %s has failed: %w", serverAddr, err)
	}

	return conn, nil
}

func (c *Cluster) ConnectLocal() error {
	c.mut.Lock()
	defer c.mut.Unlock()

	_, connected := c.connections[c.localID]
	if connected {
		return nil
	}

	member, found := c.members[c.localID]
	if !found {
		return errors.New("no local node found in the member list")
	}

	conn, err := c.createConnection(&member)
	if err != nil {
		return err
	}

	c.connections[member.ID] = conn

	return nil
}

func (c *Cluster) AddMembers(members []Member) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	newMembers := make([]Member, 0)
	for _, n := range members {
		if _, found := c.members[n.ID]; !found {
			newMembers = append(newMembers, n)
		}
	}

	wg := sync.WaitGroup{}

	wg.Add(len(newMembers))

	var failed int32

	for _, n := range newMembers {
		go func(m Member) {
			defer wg.Done()

			event := &NodeJoined{
				NodeID:     m.ID,
				NodeName:   m.Name,
				ServerAddr: m.ServerAddr,
				GossipAddr: m.GossipAddr,
			}

			if err := c.eventPub.Publish(event); err != nil {
				level.Error(c.logger).Log("msg", "failed to broadcast an event", "err", err)
				atomic.AddInt32(&failed, 1)
			}
		}(n)
	}

	wg.Wait()

	if failed > 0 && int(failed) == len(newMembers) {
		return errors.New("add members: all broadcasts failed")
	}

	for _, node := range newMembers {
		c.members[node.ID] = node

		c.eventPub.RegisterNode(&node)

		conn, err := c.createConnection(&node)
		if err != nil {
			level.Error(c.logger).Log("msg", "failed to connect to a node", "name")
			continue
		}

		c.connections[node.ID] = conn
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

	resp, err := conn.Join(ctx, &proto.JoinRequest{
		LocalMembers: func() (members []*proto.Member) {
			for _, m := range c.members {
				members = append(members, ToProtoMember(&m))
			}

			return
		}(),
	})
	if err != nil {
		return fmt.Errorf("failed ot join the cluster: %w", err)
	}

	remoteMembers := make([]Member, 0)
	for _, rn := range resp.RemoteMembers {
		remoteMembers = append(remoteMembers, FromProtoMember(rn))
	}

	c.AddMembers(remoteMembers)

	return nil
}

func (c *Cluster) Leave() error {
	c.mut.Lock()
	defer c.mut.Unlock()

	event := &NodeLeft{
		NodeID: c.localID,
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

	c.members = make(map[NodeID]Member)

	return nil
}

func (c *Cluster) SetNodeStatus(nodeID NodeID, status Status) (Status, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	node, ok := c.members[nodeID]
	if !ok {
		return 0, errors.New("no such node")
	}

	oldStatus := node.Status
	if oldStatus == status {
		return oldStatus, nil
	}

	node.Version++

	node.Status = status

	statusChanged := &NodeStatusChanged{
		NodeID:    node.ID,
		Status:    status,
		OldStatus: oldStatus,
		Version:   node.Version,
		SourceID:  c.localID,
	}

	if err := c.eventPub.Publish(statusChanged); err != nil {
		return 0, fmt.Errorf("failed to publish NodeStatusChanged event: %w", err)
	}

	c.members[nodeID] = node

	return oldStatus, nil
}
