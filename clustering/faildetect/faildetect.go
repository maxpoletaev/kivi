package faildetect

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/protobuf/types/known/emptypb"

	clust "github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/clustering/proto"
)

type clutser interface {
	NRandomNodes(n int, filter func(*clust.Node) bool) []clust.Node
	SetNodeStatus(id clust.NodeID, status clust.Status) (clust.Status, error)
}

type Detector struct {
	cluster           clutser
	logger            log.Logger
	loopDone          chan struct{}
	pingInterval      time.Duration
	pingTimeout       time.Duration
	indirectPingNodes int
}

func New(c clutser, logger log.Logger) *Detector {
	return &Detector{
		cluster:           c,
		logger:            logger,
		loopDone:          make(chan struct{}),
		pingInterval:      3 * time.Second,
		pingTimeout:       2 * time.Second,
		indirectPingNodes: 1,
	}
}

func (d *Detector) randomNode() *clust.Node {
	nodes := d.cluster.NRandomNodes(1, func(n *clust.Node) bool {
		return !n.Local
	})

	if len(nodes) == 0 {
		return nil
	}

	return &nodes[0]
}

func (d *Detector) RunLoop() {
	level.Info(d.logger).Log(
		"msg", "failure detector loop started",
		"ping_interval", d.pingInterval,
	)

	for {
		select {
		case <-time.After(d.pingInterval):
			// noop
		case <-d.loopDone:
			return
		}

		if node := d.randomNode(); node != nil {
			var err error

			switch node.Status {
			case clust.StatusAlive:
				err = d.tryAliveNode(node)
			case clust.StatusDead:
				err = d.tryDeadNode(node)
			}

			if err != nil {
				level.Error(d.logger).Log(
					"msg", "failure detector ping failed",
					"node", node.Name,
					"err", err,
				)
			}
		}
	}
}

func (d *Detector) Stop() {
	close(d.loopDone)
}

func (d *Detector) tryAliveNode(node *clust.Node) error {
	level.Debug(d.logger).Log("msg", "attempting to ping an alive node", "name", node.Name)

	directOK, err := d.directProbe(node)
	if err != nil {
		return err
	}

	if !directOK {
		level.Debug(d.logger).Log("msg", "direct ping failed, trying indirect", "name", node.Name)

		indirectOk, err := d.indirectProbe(node)
		if err != nil {
			return err
		}

		if !indirectOk {
			_, err = d.cluster.SetNodeStatus(node.ID, clust.StatusDead)
			if err != nil {
				return err
			}

			return nil
		}
	}

	return nil
}

func (d *Detector) tryDeadNode(node *clust.Node) error {
	level.Debug(d.logger).Log("msg", "attempting to ping a dead node", "name", node.Name)

	directOk, err := d.directProbe(node)
	if err != nil {
		return err
	}

	if directOk {
		level.Debug(d.logger).Log("msg", "direct ping succeeded, attempting indirect", "name", node.Name)

		indirectOk, err := d.indirectProbe(node)
		if err != nil {
			return err
		}

		if indirectOk {
			_, err := d.cluster.SetNodeStatus(node.ID, clust.StatusAlive)
			if err != nil {
				return err
			}

			return nil
		}
	}

	return nil
}

func (d *Detector) directProbe(target *clust.Node) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.pingTimeout)
	defer cancel()

	_, err := target.Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return false, nil
	}

	return true, nil
}

func (d *Detector) indirectProbe(target *clust.Node) (ok bool, _ error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.pingTimeout)
	defer cancel()

	pingNodes := d.cluster.NRandomNodes(d.indirectPingNodes, func(n *clust.Node) bool {
		return !n.Local && n.ID != target.ID && n.Status == clust.StatusAlive
	})

	if len(pingNodes) < d.indirectPingNodes {
		return false, errors.New("not enough nodes for indirect probe")
	}

	var failed int32

	wg := sync.WaitGroup{}

	wg.Add(d.indirectPingNodes)

	for idx := range pingNodes {
		go func(node *clust.Node) {
			defer wg.Done()

			req := &proto.PingRequest{
				NodeId: uint32(target.ID),
			}

			_, err := node.PingIndirect(ctx, req)
			if err != nil {
				atomic.AddInt32(&failed, 1)

				level.Warn(d.logger).Log(
					"msg", "indirect ping failed",
					"from_node", node.Name,
					"to_node", target.Name,
					"err", err,
				)
			}
		}(&pingNodes[idx])
	}

	wg.Wait()

	if failed == 0 {
		ok = true
	}

	return ok, nil
}
