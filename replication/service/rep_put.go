package service

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kiwi/internal/grpcutil"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"github.com/maxpoletaev/kiwi/replication"
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

	var (
		members   = s.members.Members()
		localConn = s.connections.Local()
		needAcks  = s.writeLevel.N(len(members))
	)

	// Do not attempt to write if we know in advance that there is not enough alive nodes.
	if countAlive(members) < needAcks {
		return nil, errNotEnoughReplicas
	}

	// The first write goes to the coordinator, which is the local node. The coordinator
	// is responsible for generating the version number, which is then send to the other nodes.
	version, err := putValue(ctx, localConn, req.Key, req.Value.Data, req.Version, true)
	if err != nil {
		return nil, err
	}

	// We already received an ack from the primary node, so skip in the map-reduce operation.
	ackedIDs := make(map[membership.NodeID]struct{})
	ackedIDs[s.members.SelfID()] = struct{}{}

	err = replication.Opts[string]{
		MinAcks:    needAcks,
		ReplicaSet: members,
		AckedIDs:   ackedIDs,
		Conns:      s.connections,
		Logger:     s.logger,
		Timeout:    s.writeTimeout,
		Background: true,
	}.MapReduce(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn nodeclient.Conn, reply *replication.NodeReply[string]) {
			version, err := putValue(ctx, conn, req.Key, req.Value.Data, version, false)
			if err != nil {
				reply.Error(err)
				return
			}

			reply.Ok(version)
		},
		func(cancel func(), nodeID membership.NodeID, version string, err error) error {
			// Cancel the whole write if one of the replicas already has a newer value.
			if grpcutil.ErrorCode(err) == codes.AlreadyExists {
				cancel()
			}

			return nil
		},
	)

	if err != nil {
		// If we did not receive enough acks, return a special error that will be
		// converted to a Unavailable response. This is done to distinguish between
		// a write that failed because of a network partition and a write that failed
		// because of a write quorum not being satisfied.
		if errors.Is(err, replication.ErrNotEnoughAcks) {
			return nil, errLevelNotSatisfied
		}

		return nil, err
	}

	return &proto.PutResponse{
		Version: version,
	}, nil
}

func putValue(
	ctx context.Context,
	conn nodeclient.Conn,
	key string,
	value []byte,
	version string,
	primary bool,
) (string, error) {
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
