package membership

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-kit/log/level"

	membershippb "github.com/maxpoletaev/kivi/membership/proto"
)

type probeResult struct {
	duration time.Duration
	status   Status
	message  string
}

func (cl *SWIMCluster) startDetector() {
	cl.wg.Add(1)

	go func() {
		defer cl.wg.Done()

		ticker := time.NewTicker(cl.probeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if cl.probeJitter > 0 {
					jitter := rand.Int63n(int64(cl.probeJitter))
					time.Sleep(time.Duration(jitter))
				}

				cl.detectFailures()
			case <-cl.stop:
				return
			}
		}
	}()
}

func (cl *SWIMCluster) pickTargetNode() *Node {
	nodes := cl.Nodes()

	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	for _, node := range nodes {
		if node.ID != cl.selfID && node.Status != StatusLeft {
			return &node
		}
	}

	return nil
}

func (cl *SWIMCluster) pickIndirectNodes(node *Node) []*Node {
	nodes := cl.Nodes()

	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	res := make([]*Node, 0, cl.indirectNodes)

	for _, n := range nodes {
		if n.ID != node.ID && n.ID != cl.selfID && n.Status == StatusHealthy {
			res = append(res, &n)
		}

		if len(res) == cl.indirectNodes {
			break
		}
	}

	return res
}

func (cl *SWIMCluster) setStatus(id NodeID, status Status, message string) {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	node, ok := cl.nodes[id]
	if !ok || node.Status == status {
		return
	}

	cl.logger.Log(
		"msg", "node status changed",
		"node_id", node.ID,
		"status", status,
		"error", message,
	)

	node.Status = status
	node.Error = ""
	node.Gen++

	if len(message) > 0 {
		node.Error = message
	}

	cl.nodes[id] = node

	cl.stateHash = 0
	for _, node := range cl.nodes {
		cl.stateHash ^= node.Hash64()
	}
}

func (cl *SWIMCluster) detectFailures() {
	target := cl.pickTargetNode()
	if target == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		directRes   *probeResult
		indirectRes *probeResult
		err         error
	)

	// Try directly ping the node and exit if the state is the same as before.
	if directRes, err = cl.directProbe(ctx, target); err != nil {
		level.Error(cl.logger).Log("msg", "direct probe failed", "node_id", target.ID, "err", err)
		return
	} else if directRes.status == target.Status {
		return
	}

	level.Info(cl.logger).Log(
		"msg", "direct probe result changed",
		"node_id", target.ID,
		"status", target.Status,
		"new_status", directRes.status,
	)

	// In case the state has changed, we need several intermediary nodes to confirm
	// the new state of the target. Yet, there might be a situation when there is not
	// enough intermediary nodes alive (e.g. when the cluster is small, or when there
	// is a network partition). In this case, we just rely on the direct probe result.
	// This is not ideal, but from out perspective, we are alive and the target is not.
	nodes := cl.pickIndirectNodes(target)
	if len(nodes) < cl.indirectNodes {
		level.Warn(cl.logger).Log("msg", "not enough intermediary nodes")
		cl.setStatus(target.ID, directRes.status, directRes.message)

		return
	}

	// Ask the intermediary nodes to ping the target on our behalf. If all the
	// intermediary nodes agree on the new state, we can safely update the state of
	// the target node.
	if indirectRes, err = cl.indirectProbe(ctx, target, nodes); err != nil {
		level.Error(cl.logger).Log("msg", "indirect probe failed", "node_id", target.ID, "err", err)
		return
	} else if indirectRes.status == target.Status {
		return
	}

	// Do nothing as long as the direct and indirect probe results differ.
	if directRes.status != indirectRes.status {
		level.Warn(cl.logger).Log(
			"msg", "local and indirect probe results differ",
			"node_id", target.ID,
			"direct_status", directRes.status,
			"indirect_status", indirectRes.status,
		)

		return
	}

	cl.setStatus(target.ID, directRes.status, directRes.message)
}

func (cl *SWIMCluster) directProbe(ctx context.Context, node *Node) (*probeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, cl.probeTimeout)
	defer cancel()

	start := time.Now()

	conn, err := cl.ConnContext(ctx, node.ID)
	if err != nil {
		return &probeResult{ //nolint:nilerr
			duration: time.Since(start),
			status:   StatusUnhealthy,
			message:  err.Error(),
		}, nil
	}

	// Lightweight ping message which also carries the state hash of the sender.
	// It will be later used to determine if the full state exchange is required.
	pingRes, err := conn.Membership.Ping(ctx, &membershippb.PingRequest{})
	if err != nil {
		return &probeResult{ //nolint:nilerr
			duration: time.Since(start),
			status:   StatusUnhealthy,
			message:  err.Error(),
		}, nil
	}

	// In case of a difference between the local and remote state hashes, the full
	// state exchange is performed. This would merge the states of both nodes and
	// make them consistent.
	if pingRes.StateHash != cl.StateHash() {
		level.Info(cl.logger).Log("msg", "performing state exchange", "node_id", node.ID)

		stateRes, err := conn.Membership.PullPushState(ctx, &membershippb.PullPushStateRequest{
			Nodes: ToProtoNodeList(cl.Nodes()),
		})

		if err != nil {
			level.Error(cl.logger).Log("msg", "state exchange failed", "node_id", node.ID, "err", err)
			return nil, err
		}

		if len(stateRes.Nodes) > 0 {
			nodes := FromProtoNodeList(stateRes.Nodes)
			cl.ApplyState(nodes, node.ID)
		}
	}

	return &probeResult{
		duration: time.Since(start),
		status:   StatusHealthy,
	}, nil
}

func (cl *SWIMCluster) indirectProbe(ctx context.Context, target *Node, nodes []*Node) (*probeResult, error) {
	ctx, cancel := context.WithTimeout(ctx, cl.probeTimeout*3)
	defer cancel()

	type voteResult struct {
		status Status
		err    error
	}

	votesCh := make(chan voteResult, len(nodes))
	wg := sync.WaitGroup{}
	wg.Add(len(nodes))

	for i := range nodes {
		go func(node *Node) {
			defer wg.Done()

			status, err := func() (Status, error) {
				conn, err := cl.ConnContext(ctx, node.ID)
				if err != nil {
					return 0, err
				}

				res, err := conn.Membership.PingIndirect(ctx, &membershippb.PingIndirectRequest{
					Timeout: int64(cl.probeTimeout),
					NodeId:  uint32(target.ID),
				})

				if err != nil {
					return 0, err
				}

				return FromProtoStatusMap[res.Status], nil
			}()

			votesCh <- voteResult{
				status: status,
				err:    err,
			}
		}(nodes[i])
	}

	go func() {
		wg.Wait()
		close(votesCh)
	}()

	votes := make(map[Status]int)
	for vote := range votesCh {
		votes[vote.status]++
	}

	level.Info(cl.logger).Log(
		"msg", "indirect probe votes",
		"target_node", target.ID,
		"healthy", votes[StatusHealthy],
		"unhealthy", votes[StatusUnhealthy],
	)

	if votes[StatusHealthy] == len(nodes) {
		return &probeResult{status: StatusUnhealthy}, nil
	} else if votes[StatusUnhealthy] == len(nodes) {
		return &probeResult{status: StatusHealthy}, nil
	}

	return nil, fmt.Errorf("not enough votes")
}
