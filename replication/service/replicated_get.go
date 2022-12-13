package service

import (
	"context"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/maxpoletaev/kv/internal/generic"
	"github.com/maxpoletaev/kv/internal/grpcutil"
	"github.com/maxpoletaev/kv/internal/set"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/replication/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

func (s *ReplicationService) validateGetRequest(req *proto.GetRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) ReplicatedGet(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if err := s.validateGetRequest(req); err != nil {
		return nil, err
	}

	replicas := s.cluster.Members()

	minAcks := s.readLevel.N(len(replicas))

	if countAlive(replicas) < minAcks {
		return nil, errNotEnoughReplicas
	}

	readCtx, cacnelRead := context.WithTimeout(ctx, s.readTimeout)

	readResults := make(chan *nodeGetResult)

	wg := sync.WaitGroup{}

	for i := range replicas {
		replica := &replicas[i]
		if !replica.IsReacheable() {
			continue
		}

		wg.Add(1)

		go func(replica *membership.Member) {
			defer wg.Done()

			select {
			case <-readCtx.Done():
				return
			default:
			}

			conn, err := s.cluster.Conn(replica.ID)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed to get connection", "name", replica.Name, "err", err)
				return
			}

			res, err := conn.Get(readCtx, &storagepb.GetRequest{Key: req.Key})
			if err != nil {
				if !grpcutil.IsCanceled(err) {
					s.logger.Log("msg", "failed to read from replica", "name", replica.Name, "err", err)
				}

				return
			}

			readResults <- &nodeGetResult{
				NodeID: replica.ID,
				Values: res.Value,
			}
		}(replica)
	}

	go func() {
		wg.Wait()
		cacnelRead()
		close(readResults)
	}()

	// All values we received from the replicas.
	receivedValues := make([]nodeValue, 0)

	// Keeping track of replied and empty replicas for further repair.
	repliedReplicas := make(set.Set[membership.NodeID])
	emptyReplicas := make(set.Set[membership.NodeID])

readloop:
	for {
		select {
		case r := <-readResults:
			if r != nil {
				repliedReplicas.Add(r.NodeID)

				if len(r.Values) == 0 {
					emptyReplicas.Add(r.NodeID)
				}

				for i := range r.Values {
					receivedValues = append(receivedValues, nodeValue{
						NodeID:         r.NodeID,
						VersionedValue: r.Values[i],
					})
				}

				// Got enough here, no need to wait for other reads.
				if len(repliedReplicas) == minAcks {
					cacnelRead()
					break readloop
				}
			} else {
				return nil, errLevelNotSatisfied
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Discard all outdated values and keep only the latest conflicting ones.
	mergedValues := mergeValues(receivedValues)

	// Replicas that did not retuned any value or returned an outdated value should be repaired.
	repairSet := set.FromSlice(mergedValues.StaleReplicas).And(emptyReplicas)

	// Read repair, only if there are no conflicts.
	if len(mergedValues.Values) == 1 && len(repairSet) > 0 {
		repairCtx, cancelRepair := context.WithTimeout(ctx, s.writeTimeout)
		repairResults := make(chan *nodePutResult)
		data := mergedValues.Values[0].Data
		wg := sync.WaitGroup{}

		for i := range replicas {
			replica := &replicas[i]
			if !repairSet.Has(replica.ID) {
				continue
			}

			wg.Add(1)

			go func(replica *membership.Member) {
				defer wg.Done()

				conn, err := s.cluster.Conn(replica.ID)
				if err != nil {
					level.Warn(s.logger).Log("msg", "failed to get connection", "name", replica.Name, "err", err)
					return
				}

				version, err := put(repairCtx, conn, req.Key, data, mergedValues.Version, false)
				if err != nil {
					s.logger.Log("msg", "failed to repair", "replica", replica.Name, "err", err)
					return
				}

				repairResults <- &nodePutResult{
					NodeID:  replica.ID,
					Version: version,
				}
			}(replica)
		}

		go func() {
			wg.Wait()
			cancelRepair()
			close(repairResults)
		}()

	repairloop:
		for {
			select {
			case r := <-repairResults:
				if r != nil {
					repairSet.Remove(r.NodeID)
					if len(repairSet) == 0 {
						break repairloop
					}
				} else {
					return nil, errLevelNotSatisfied
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	protoValues := make([]*proto.Value, 0, len(mergedValues.Values))
	for _, value := range mergedValues.Values {
		protoValues = append(protoValues, &proto.Value{Data: value.VersionedValue.Data})
	}

	return &proto.GetResponse{
		Version: mergedValues.Version,
		Values:  protoValues,
	}, nil
}

type mergeResult struct {
	Values        []nodeValue
	Version       vclock.Vector
	StaleReplicas []membership.NodeID
}

func mergeValues(values []nodeValue) mergeResult {
	mergedVersion := make(vclock.Vector)
	for _, v := range values {
		mergedVersion = vclock.Merge(mergedVersion, v.Version)
	}

	if len(values) < 2 {
		return mergeResult{
			Values:  values,
			Version: mergedVersion,
		}
	}

	var staleReplicas []membership.NodeID

	uniqueValues := make(map[uint32]nodeValue)

	// Find out the highest version.
	highestVer := values[0].Version
	for _, v := range values[1:] {
		if vclock.Compare(highestVer, v.Version) == vclock.Before {
			highestVer = v.Version
		}
	}

	for _, v := range values {
		// Ignore the values that clearly precedes the highest version.
		if vclock.Compare(v.Version, highestVer) == vclock.Before {
			staleReplicas = append(staleReplicas, v.NodeID)
			continue
		}

		// Keep unique values only, based on version hash.
		h := vclock.Vector(v.Version).Hash()
		if _, ok := uniqueValues[h]; !ok {
			uniqueValues[h] = v
		}
	}

	return mergeResult{
		Values:        generic.MapValues(uniqueValues),
		Version:       mergedVersion,
		StaleReplicas: staleReplicas,
	}
}
