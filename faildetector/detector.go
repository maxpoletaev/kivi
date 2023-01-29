package faildetector

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/errgroup"

	"github.com/maxpoletaev/kiwi/faildetector/proto"
	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/membership"
)

// errNotEnoughIndirectNodes is returned when there are not enough alive intermediate
// nodes to perform indirect ping. It probably means one of the following:
//
//  1. We are the one who is faulty. In this case we will be marked faulty by other nodes,
//     but from our perspective we are still healthy and everyone else is dead.
//
//  2. There are just not enough nodes in the cluster. Say, we have 3 nodes and one of them
//     is faulty. In this case we will be able to ping only one node, but we need two to
//     perform indirect ping.
//
// In both cases the algorithm should be able make progress and recover from the situation,
// so in case we are not able to perform indirect ping due to this error, the node will still
// be marked as healthy or faulty depending on the result of direct ping.
var errNotEnoughIndirectNodes = fmt.Errorf("not enough indirect nodes")

type Detector struct {
	members           Memberlist
	connections       ConnRegistry
	logger            log.Logger
	pingInterval      time.Duration
	pingTimeout       time.Duration
	indirectPingNodes int
}

func New(members Memberlist, connections ConnRegistry, logger log.Logger, opts ...Option) *Detector {
	d := &Detector{
		logger:            logger,
		members:           members,
		connections:       connections,
		pingInterval:      5 * time.Second,
		pingTimeout:       2 * time.Second,
		indirectPingNodes: 2,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

func (d *Detector) pickRandomMember() *membership.Member {
	members := d.members.Members()
	self := d.members.Self()
	generic.Shuffle(members)

	for _, member := range members {
		if member.ID != self.ID {
			return &member
		}
	}

	return nil
}

func (d *Detector) RunLoop(ctx context.Context) {
	level.Info(d.logger).Log(
		"msg", "failure detector loop started",
		"ping_interval", d.pingInterval,
	)

	for {
		select {
		case <-time.After(d.pingInterval):
			// noop
		case <-ctx.Done():
			return
		}

		// Select random member for ping.
		member := d.pickRandomMember()
		if member == nil {
			continue
		}

		var err error

		switch member.Status {
		case membership.StatusHealthy:
			err = d.pingHealthyMember(member)
		case membership.StatusFaulty:
			err = d.pingFaultyMember(member)
		}

		if err != nil {
			level.Error(d.logger).Log(
				"msg", "failure detector ping failed",
				"node", member.Name,
				"err", err,
			)
		}
	}
}

func (d *Detector) markHealthy(member *membership.Member) error {
	if _, err := d.members.SetStatus(member.ID, membership.StatusHealthy); err != nil {
		return fmt.Errorf("failed to change node status: %w", err)
	}

	return nil
}

func (d *Detector) markFaulty(member *membership.Member) error {
	if _, err := d.members.SetStatus(member.ID, membership.StatusFaulty); err != nil {
		return fmt.Errorf("failed to change node status: %w", err)
	}

	return nil
}

func (d *Detector) pingHealthyMember(member *membership.Member) error {
	directOK, err := d.directProbe(member)
	if err != nil {
		return fmt.Errorf("direct probe failed: %w", err)
	}

	if !directOK {
		indirectOk, err := d.indirectProbe(member)
		if err != nil {
			if errors.Is(err, errNotEnoughIndirectNodes) {
				return d.markFaulty(member)
			}

			return fmt.Errorf("indirect probe failed: %w", err)
		}

		if !indirectOk {
			return d.markFaulty(member)
		}
	}

	return nil
}

func (d *Detector) pingFaultyMember(member *membership.Member) error {
	directOk, err := d.directProbe(member)
	if err != nil {
		return fmt.Errorf("direct probe failed: %w", err)
	}

	if directOk {
		indirectOk, err := d.indirectProbe(member)
		if err != nil {
			if errors.Is(err, errNotEnoughIndirectNodes) {
				return d.markHealthy(member)
			}

			return fmt.Errorf("indirect probe failed: %w", err)
		}

		if indirectOk {
			return d.markHealthy(member)
		}
	}

	return nil
}

func (d *Detector) directProbe(member *membership.Member) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.pingTimeout)
	defer cancel()

	conn, err := d.connections.Get(member.ID)
	if err != nil {
		return false, fmt.Errorf("failed to get connection: %w", err)
	}

	resp, err := conn.PingDirect(ctx)
	if err != nil {
		return false, nil // nolint:nilerr
	}

	return resp.Alive, nil
}

func (d *Detector) indirectProbe(target *membership.Member) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.pingTimeout)
	defer cancel()

	errg := errgroup.Group{}
	self := d.members.Self()

	req := &proto.PingRequest{
		MemberId: uint32(target.ID),
	}

	// Members for indirect ping are selected randomly.
	members := d.members.Members()
	generic.Shuffle(members)

	var (
		scheduledCount int32
		aliveCount     int32
	)

	for i := range members {
		member := &members[i]

		// Skip self, target and members that are already dead.
		if member.ID == self.ID || member.ID == target.ID || !member.IsReachable() {
			continue
		}

		conn, err := d.connections.Get(member.ID)
		if err != nil {
			level.Warn(d.logger).Log("msg", "unable to get connection", "node", member.Name, "err", err)
			continue
		}

		scheduledCount++

		errg.Go(func() error {
			resp, err := conn.PingIndirect(ctx, req)
			if err != nil {
				return err
			}

			if resp.Alive {
				atomic.AddInt32(&aliveCount, 1)
			}

			return nil
		})

		// Stop scheduling once we have enough nodes.
		if scheduledCount == int32(d.indirectPingNodes) {
			break
		}
	}

	if err := errg.Wait(); err != nil {
		return false, err
	}

	if scheduledCount < int32(d.indirectPingNodes) {
		return false, errNotEnoughIndirectNodes
	}

	// To be considered alive, all intermediate nodes must report that the target is alive.
	if aliveCount == scheduledCount {
		return true, nil
	}

	return false, nil
}
