package membership

import (
	"encoding/binary"
	"hash/fnv"
)

type NodeID uint32

// Node represents a single cluster member.
type Node struct {
	ID          NodeID
	RunID       int64
	Name        string
	PublicAddr  string
	LocalAddr   string
	Error       string
	Status      Status
	LocalStatus Status
	Gen         uint32
}

// IsReachable returns true if the node is reachable.
func (n *Node) IsReachable() bool {
	return n.Status == StatusHealthy
}

// Hash64 returns a 64-bit hash of the node.
func (n *Node) Hash64() uint64 {
	h := fnv.New64a()
	_ = binary.Write(h, binary.LittleEndian, n.ID)
	_ = binary.Write(h, binary.LittleEndian, n.RunID)
	_ = binary.Write(h, binary.LittleEndian, n.Gen)

	return h.Sum64()
}
