package service

import (
	"context"

	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeclient"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/proto"
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

func (s *ReplicationServer) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if err := validateDeleteRequest(req); err != nil {
		return nil, err
	}

	var (
		members  = s.cluster.Nodes()
		needAcks = s.writeLevel.N(len(members))
	)

	repliedIDs := make(map[membership.NodeID]struct{})
	repliedIDs[s.cluster.SelfID()] = struct{}{}
	localConn := s.cluster.LocalConn()

	version, err := putTombstone(
		ctx, localConn, req.Key, req.Version, true)
	if err != nil {
		return nil, err
	}

	err = replication.Opts[string]{
		ReplicaSet: members,
		MinAcks:    needAcks,
		AckedIDs:   repliedIDs,
		Cluster:    s.cluster,
		Logger:     s.logger,
		Timeout:    s.writeTimeout,
		Background: true,
	}.Distribute(
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

	return &proto.DeleteResponse{
		Version: version,
	}, nil
}

func putTombstone(ctx context.Context, conn nodeclient.Conn, key, version string, primary bool) (string, error) {
	resp, err := conn.StoragePut(ctx, key, nodeclient.VersionedValue{
		Version:   version,
		Tombstone: true,
	}, primary)

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}
