package service

import (
	"context"

	"github.com/maxpoletaev/kiwi/internal/grpcutil"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"github.com/maxpoletaev/kiwi/replication"
	"github.com/maxpoletaev/kiwi/replication/proto"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
	"google.golang.org/grpc/codes"
)

func validateDeleteRequest(req *proto.DeleteRequest) error {
	if req.Key == "" {
		return errMissingKey
	}

	if req.Version == "" {
		return errMissingVersion
	}

	return nil
}

func (s *ReplicationService) ReplicatedDelete(
	ctx context.Context, req *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	if err := validateDeleteRequest(req); err != nil {
		return nil, err
	}

	var (
		members  = s.members.Members()
		needAcks = s.writeLevel.N(len(members))
	)

	repliedIDs := make(map[membership.NodeID]struct{})
	repliedIDs[s.members.SelfID()] = struct{}{}
	localConn := s.connections.Local()

	version, err := putTombstone(
		ctx, localConn, req.Key, req.Version, true)
	if err != nil {
		return nil, err
	}

	err = replication.Opts[string]{
		ReplicaSet: members,
		MinAcks:    needAcks,
		AckedIDs:   repliedIDs,
		Conns:      s.connections,
		Logger:     s.logger,
	}.MapReduce(
		ctx,
		func(
			ctx context.Context,
			nodeID membership.NodeID,
			conn nodeclient.Conn,
			reply *replication.NodeReply[string],
		) {
			version, err := putTombstone(ctx, conn, req.Key, version, false)
			if err != nil {
				reply.Error(err)
				return
			}

			reply.Ok(version)
		},
		func(cancel func(), nodeID membership.NodeID, res string, err error) error {
			if err != nil {
				if grpcutil.ErrorCode(err) == codes.AlreadyExists {
					cancel()
				}
			}

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &proto.DeleteResponse{}, nil
}

func putTombstone(ctx context.Context, conn nodeclient.Conn, key, version string, primary bool) (string, error) {
	resp, err := conn.Put(ctx, &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Version:   version,
			Tombstone: true,
		},
	})
	if err != nil {
		return "", err
	}

	return resp.Version, nil
}
