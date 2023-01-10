package clust

import (
	"context"

	"github.com/maxpoletaev/kv/membership/proto"
	"golang.org/x/sync/errgroup"
)

// JoinClusters joins two clusters together by adding all members of the left
// cluster to the right cluster and vice versa. If any of the calls to Join fail,
// the function returns an error, however, there is no guarantee that the membership
// tables of both clusters will be consistent afther an error. For example, if the
// left cluster is joined to the right cluster, but the right cluster fails to join
// the left cluster, the left cluster will have all of the members of the right cluster
// in its membership table, but the right cluster will not know about the left cluster.
// This is a known issue and should be adressed with periodic membership table syncs.
func JoinClusters(ctx context.Context, left, right Conn) error {
	leftResp, err := left.Members(ctx)
	if err != nil {
		return err
	}

	rightResp, err := right.Members(ctx)
	if err != nil {
		return err
	}

	errg := errgroup.Group{}

	errg.Go(func() error {
		_, err := right.Join(ctx, &proto.JoinRequest{
			MembersToAdd: leftResp.Members,
		})
		return err
	})

	errg.Go(func() error {
		_, err := left.Join(ctx, &proto.JoinRequest{
			MembersToAdd: rightResp.Members,
		})
		return err
	})

	return errg.Wait()
}
