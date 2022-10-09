package service

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/internal/grpcutil"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/replication/consistency"
	"github.com/maxpoletaev/kv/replication/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

func (s *CoordinatorService) ReplicatedPut(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	replicas := make([]clustering.Node, 0)
	for _, node := range s.cluster.Nodes() {
		if node.Status == clustering.StatusAlive && !node.Local {
			replicas = append(replicas, node)
		}
	}

	minAcks := s.writeConsistency.N(len(replicas))

	if len(replicas) < minAcks {
		return nil, errNotEnoughReplicas
	}

	// Local node coordinates the write.
	leader := s.cluster.LocalNode()

	criterr := make(chan error, 1)
	putResults := make(chan *replicaPutResult, len(replicas))

	// Initial write goes to the local replica.
	// This one should increment the verstion vector which then will be replicated to other replicas.
	newVersion, err := put(ctx, leader, req.Key, req.Value.Data, req.Version, true)
	if err != nil {
		s.logger.Log("msg", "primary replica write failed", "err", err)
		return nil, status.Errorf(codes.Unavailable, "failed to write to primary replica: %s", err)
	}

	// To provide best-effort, we continue writing to replicas in background even after
	// we have received enough acknowledgements to satisfy the desired consistency level.
	// Therefore, the context should not be cancelled until the timeout fires or unless
	// there is a critical error that makes the current write pointless.
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), s.writeTimeout)

	wg := sync.WaitGroup{}
	wg.Add(len(replicas))

	for _, replica := range replicas {
		go func(replica clustering.Node) {
			defer wg.Done()

			select {
			case <-writeCtx.Done():
				return
			default:
			}

			version, err := put(writeCtx, replica, req.Key, req.Value.Data, newVersion, false)
			if err != nil {
				if grpcutil.ErrorCode(err) == codes.AlreadyExists {
					// Some replicas already have a newer value. There is no point to continue the operation.
					// The channel write is non-blocking since the error is only read once.
					select {
					case criterr <- err:
					default:
					}
				}

				s.logger.Log("msg", "write to replica has failed", "err", err)

				return
			}

			putResults <- &replicaPutResult{
				ReplicaName: replica.Name,
				Version:     version,
			}
		}(replica)
	}

	go func() {
		wg.Wait()
		cancelWrite()
		close(criterr)
		close(putResults)
	}()

	// At this point we already have one ack from the primary...
	numAcks := 1

	// ...which is enough to fulfill the ConsistencyLevelOne.
	// The write will continue in the background, though.
	if s.writeConsistency == consistency.LevelOne {
		return &proto.PutResponse{
			Version: newVersion,
		}, nil
	}

	for {
		// Block until either:
		// - We have enough acknowledgements from replicas to satisfy the consistency level.
		// - We run out of replicas and haven’t got enough responses.
		// - There is a critical error in the criterr channel.

		select {
		case r := <-putResults:
			if r != nil {
				numAcks++

				if numAcks == minAcks {
					return &proto.PutResponse{
						Version: newVersion,
					}, nil
				}
			} else {
				return nil, errLevelNotSatisfied
			}
		case err := <-criterr:
			if err != nil {
				cancelWrite()
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func put(ctx context.Context, replica clustering.Node, key string,
	value []byte, version vclock.Vector, primary bool) (vclock.Vector, error) {

	req := &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Version: version,
			Data:    value,
		},
	}

	resp, err := replica.Put(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Version, nil
}
