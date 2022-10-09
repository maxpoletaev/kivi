package service

import (
	"context"
	"sync"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/internal/collections"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/replication/proto"
	spb "github.com/maxpoletaev/kv/storage/proto"
)

func (s *CoordinatorService) ReplicatedGet(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	replicas := make([]clustering.Node, 0)
	for _, node := range s.cluster.Nodes() {
		if !node.Local && node.Status == clustering.StatusAlive {
			replicas = append(replicas, node)
		}
	}

	minReadAcks := s.readConsistency.N(len(replicas))
	if len(replicas) < minReadAcks {
		return nil, errLevelNotSatisfied
	}

	readCtx, cacnelRead := context.WithTimeout(ctx, s.readTimeout)
	readResults := make(chan *replicaGetResult, len(replicas))

	wg := sync.WaitGroup{}
	wg.Add(len(replicas))

	for _, replica := range replicas {
		go func(replica clustering.Node) {
			defer wg.Done()

			select {
			case <-readCtx.Done():
				return
			default:
			}

			res, err := replica.Get(readCtx, &spb.GetRequest{Key: req.Key})
			if err != nil {
				s.logger.Log("msg", "failed to read from replica", "name", replica.Name)
				return
			}

			readResults <- &replicaGetResult{
				ReplicaName: replica.Name,
				Values:      res.Value,
			}
		}(replica)
	}

	go func() {
		wg.Wait()
		cacnelRead()
		close(readResults)
	}()

	// All values we received from the replicas.
	replicaValues := make([]replicaValue, 0)

	// Keeping track of replied and outdated replicas for further repair.
	repliedReplicas := make(collections.Set[string])
	staleReplicas := make(collections.Set[string])

loop:
	for {
		select {
		case r := <-readResults:
			if r != nil {
				repliedReplicas.Add(r.ReplicaName)

				if len(r.Values) == 0 {
					staleReplicas.Add(r.ReplicaName)
				}

				for i := range r.Values {
					replicaValues = append(replicaValues, replicaValue{
						ReplicaName:    r.ReplicaName,
						VersionedValue: r.Values[i],
					})
				}

				if len(repliedReplicas) == minReadAcks {
					cacnelRead() // Got enough here, no need to wait for other reads.
					break loop
				}
			} else {
				return nil, errLevelNotSatisfied
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// If a replica has returned an outdated value, it should be marked for repair.
	patch := mergePrepare(replicaValues)
	replicaValues = mergeApply(replicaValues, patch)
	staleReplicas = staleReplicas.And(patch.OutdatedReplicas)

	// Create a new version taking the maximum of all versions.
	mergedVersion := make(vclock.Vector, len(replicas))
	mergedValues := make([]*proto.Value, 0, len(replicaValues))
	for _, value := range replicaValues {
		mergedVersion = vclock.Merge(mergedVersion, value.Version)
		mergedValues = append(mergedValues, &proto.Value{Data: value.VersionedValue.Data})
	}

	if len(replicaValues) == 1 && len(staleReplicas) > 0 {
		repairCtx, cancelRepair := context.WithTimeout(ctx, s.writeTimeout)
		repairResults := make(chan *replicaPutResult)
		data := replicaValues[0].Data

		wg := sync.WaitGroup{}
		wg.Add(len(staleReplicas))

		for _, replica := range replicas {
			if staleReplicas.Has(replica.Name) {
				go func(replica clustering.Node) {
					defer wg.Done()

					ver, err := put(repairCtx, replica, req.Key, data, mergedVersion, false)
					if err != nil {
						s.logger.Log("msg", "failed to repair", "replica", replica.Name)
						return
					}

					repairResults <- &replicaPutResult{
						ReplicaName: replica.Name,
						Version:     ver,
					}
				}(replica)
			}
		}

		go func() {
			wg.Wait()
			cancelRepair()
			close(repairResults)
		}()

	loop2:
		for {
			select {
			case r := <-repairResults:
				if r != nil {
					staleReplicas.Remove(r.ReplicaName)

					if len(staleReplicas) == 0 {
						break loop2
					}
				} else {
					return nil, errLevelNotSatisfied
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return &proto.GetResponse{
		Values:  mergedValues,
		Version: mergedVersion,
	}, nil
}

type preparedMerge struct {
	RemovedIndices   collections.Set[int]
	OutdatedReplicas collections.Set[string]
}

func mergePrepare(values []replicaValue) preparedMerge {
	indicesToRemove := make(collections.Set[int])
	outdatedReplicas := make(collections.Set[string])

	// FIXME: O(n^2) here
	for i := 0; i < len(values); i++ {
		replicaName := values[i].ReplicaName

		for j := i + 1; j < len(values); j++ {
			switch vclock.Compare(values[i].Version, values[j].Version) {
			case vclock.Before:
				outdatedReplicas.Add(replicaName)
				indicesToRemove.Add(i)
			case vclock.Equal:
				// Vectors are equal means that the replica is up to date
				// but its value should be dropped as a duplicate.
				indicesToRemove.Add(i)
			}
		}
	}

	return preparedMerge{
		RemovedIndices:   indicesToRemove,
		OutdatedReplicas: outdatedReplicas,
	}
}

func mergeApply(values []replicaValue, patch preparedMerge) []replicaValue {
	merged := make([]replicaValue, 0, len(values))

	for i, a := range values {
		if !patch.RemovedIndices.Has(i) {
			merged = append(merged, a)
		}
	}

	return merged
}
