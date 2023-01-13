package service

import (
	"context"
	"sync"

	"github.com/go-kit/log/level"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kiwi/internal/grpcutil"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"github.com/maxpoletaev/kiwi/replication/consistency"
	"github.com/maxpoletaev/kiwi/replication/proto"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
)

func (s *ReplicationService) validatePutRequest(req *proto.PutRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) ReplicatedPut(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if err := s.validatePutRequest(req); err != nil {
		return nil, err
	}

	members := s.members.Members()
	acksLeft := s.writeLevel.N(len(members))

	if countAlive(members) < acksLeft {
		return nil, errNotEnoughReplicas
	}

	criterr := make(chan error, 1)
	localMember := s.members.Self()

	localConn, err := s.connections.Get(localMember.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "not connected to self: %s", err)
	}

	putResults := make(chan *nodePutResult, len(members))

	// Initial write goes to the local node which increments the verstion vector.
	newVersion, err := put(ctx, localConn, req.Key, req.Value.Data, req.Version, true)
	if err != nil {
		s.logger.Log("msg", "primary write failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to write to primary: %s", err)
	}

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), s.writeTimeout)

	wg := sync.WaitGroup{}
	wg.Add(len(members))

	// The write is then replicated across other nodes.
	for i := range members {
		replica := &members[i]

		if !replica.IsReacheable() || replica.ID == localMember.ID {
			wg.Done()
			continue
		}

		go func(member *membership.Member) {
			defer wg.Done()

			select {
			case <-writeCtx.Done():
				return
			default:
			}

			conn, err := s.connections.Get(replica.ID)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed to get connection", "name", replica.Name, "err", err)
				return
			}

			version, err := put(writeCtx, conn, req.Key, req.Value.Data, newVersion, false)
			if err != nil {
				if grpcutil.ErrorCode(err) == codes.AlreadyExists {
					// Some replicas already have a newer value, there is no point
					// to continue the operation. The channel write is non-blocking
					// since it will only be read once.
					select {
					case criterr <- err:
					default:
					}
				}

				level.Warn(s.logger).Log(
					"msg", "write to replica has failed",
					"replica", replica.Name,
					"key", req.Key,
					"err", err,
				)

				return
			}

			putResults <- &nodePutResult{
				NodeID:  replica.ID,
				Version: version,
			}
		}(replica)
	}

	// To provide best-effort, we continue writing to replicas in the background even after
	// we have received enough acknowledgements to satisfy the desired consistency level.
	// The write context should not be cancelled until the timeout fires (or unless
	// there is a critical error that makes the current write pointless).
	go func() {
		wg.Wait()
		cancelWrite()
		close(criterr)
		close(putResults)
	}()

	// At this point we already have one ack from the local node...
	acksLeft--

	// ...which is enough to fulfill the consistency.One level.
	if s.writeLevel == consistency.One {
		return &proto.PutResponse{
			Version: newVersion,
		}, nil
	}

	for {
		// Block until either:
		//  * We have enough acknowledgements from replicas to satisfy the consistency level.
		//  * We run out of replicas and haven’t got enough acknowledgements.
		//  * A critical error has occured in the criterr channel.

		select {
		case r := <-putResults:
			if r != nil {
				acksLeft--

				if acksLeft == 0 {
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

func put(ctx context.Context, conn nodeclient.Conn, key string,
	value []byte, version string, primary bool) (string, error) {

	req := &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Version: version,
			Data:    value,
		},
	}

	resp, err := conn.Put(ctx, req)
	if err != nil {
		return "", err
	}

	return resp.Version, nil
}
