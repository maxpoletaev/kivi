package main

import (
	"context"

	"github.com/maxpoletaev/kiwi/membership/proto"
	"github.com/maxpoletaev/kiwi/nodeclient"
)

// joinClusters joins two clusters together by adding all members of the left
// cluster to the right cluster and vice versa. If any of the calls to Join fail,
// the function returns an error, however, there is no guarantee that the membership
// tables of both clusters will be consistent afther an error. For example, if the
// left cluster is joined to the right cluster, but the right cluster fails to join
// the left cluster, the left cluster will have all of the members of the right cluster
// in its membership table, but the right cluster will not know about the left cluster.
func joinClusters(ctx context.Context, local, remote nodeclient.Conn) error {
	localResp, err := local.Members(ctx)
	if err != nil {
		return err
	}

	remoteResp, err := remote.Members(ctx)
	if err != nil {
		return err
	}

	if _, err := remote.Join(ctx, &proto.JoinRequest{
		MembersToAdd: localResp.Members,
	}); err != nil {
		return err
	}

	if _, err := local.Join(ctx, &proto.JoinRequest{
		MembersToAdd: remoteResp.Members,
	}); err != nil {
		return err
	}

	return nil
}
