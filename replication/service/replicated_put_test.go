package service

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kiwi/internal/vclock"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"github.com/maxpoletaev/kiwi/replication"
	"github.com/maxpoletaev/kiwi/replication/consistency"
	"github.com/maxpoletaev/kiwi/replication/proto"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
)

func TestReplicatedPut(t *testing.T) {
	tests := map[string]struct {
		setupCluster func(ctrl *gomock.Controller, conns *replication.MockConnRegistry, ml *MockMemberlist)
		writeLevel   consistency.Level
		req          *proto.PutRequest
		want         *proto.PutResponse
		wantCode     codes.Code
		wantErr      error
		skip         bool
	}{
		"OneOfThreeNodesInQuorumFails": {
			writeLevel: consistency.Quorum,
			setupCluster: func(ctrl *gomock.Controller, conns *replication.MockConnRegistry, ml *MockMemberlist) {
				members := []membership.Member{
					{ID: 1, Name: "node1", Status: membership.StatusHealthy},
					{ID: 2, Name: "node2", Status: membership.StatusHealthy},
					{ID: 3, Name: "node3", Status: membership.StatusHealthy},
				}

				ml.EXPECT().Members().Return(members)
				ml.EXPECT().SelfID().Return(membership.NodeID(1))

				conn1 := nodeclient.NewMockConn(ctrl)
				conn1.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key:     "key",
					Primary: true,
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(),
						Data:    []byte("value"),
					},
				}).Return(&storagepb.PutResponse{
					Version: vclock.NewEncoded(vclock.V{1: 1}),
				}, nil).MaxTimes(1)

				conn2 := nodeclient.NewMockConn(ctrl)
				conn2.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(&storagepb.PutResponse{
					Version: vclock.NewEncoded(vclock.V{1: 1}),
				}, nil).MaxTimes(1)

				conn3 := nodeclient.NewMockConn(ctrl)
				conn3.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(nil, assert.AnError).MaxTimes(1)

				conns.EXPECT().Local().Return(conn1).MaxTimes(1)
				conns.EXPECT().Get(membership.NodeID(2)).Return(conn2, nil).MaxTimes(1)
				conns.EXPECT().Get(membership.NodeID(3)).Return(conn3, nil).MaxTimes(1)
			},
			req: &proto.PutRequest{
				Key:     "key",
				Version: vclock.NewEncoded(),
				Value:   &proto.Value{Data: []byte("value")},
			},
			want: &proto.PutResponse{
				Version: vclock.NewEncoded(vclock.V{1: 1}),
			},
		},
		"TwoOfThreeNodesInQuorumFail": {
			writeLevel: consistency.Quorum,
			setupCluster: func(ctrl *gomock.Controller, conns *replication.MockConnRegistry, ml *MockMemberlist) {
				members := []membership.Member{
					{ID: 1, Name: "node1", Status: membership.StatusHealthy},
					{ID: 2, Name: "node2", Status: membership.StatusHealthy},
					{ID: 3, Name: "node3", Status: membership.StatusHealthy},
				}

				ml.EXPECT().Members().Return(members)
				ml.EXPECT().SelfID().Return(membership.NodeID(1))

				conn1 := nodeclient.NewMockConn(ctrl)
				conn1.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key:     "key",
					Primary: true,
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(),
						Data:    []byte("value"),
					},
				}).Return(&storagepb.PutResponse{
					Version: vclock.NewEncoded(vclock.V{1: 1}),
				}, nil).MaxTimes(1)

				conn2 := nodeclient.NewMockConn(ctrl)
				conn2.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(nil, assert.AnError).MaxTimes(1)

				conn3 := nodeclient.NewMockConn(ctrl)
				conn3.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(nil, assert.AnError).MaxTimes(1)

				conns.EXPECT().Local().Return(conn1).MaxTimes(1)
				conns.EXPECT().Get(membership.NodeID(2)).Return(conn2, nil).MaxTimes(1)
				conns.EXPECT().Get(membership.NodeID(3)).Return(conn3, nil).MaxTimes(1)
			},
			req: &proto.PutRequest{
				Key:     "key",
				Version: vclock.NewEncoded(),
				Value:   &proto.Value{Data: []byte("value")},
			},
			wantCode: codes.Unavailable,
			wantErr:  errLevelNotSatisfied,
		},
		"TwoOfThreeNodesInQuorumAreFaulty": {
			writeLevel: consistency.Quorum,
			setupCluster: func(ctrl *gomock.Controller, conns *replication.MockConnRegistry, ml *MockMemberlist) {
				conn := nodeclient.NewMockConn(ctrl)
				conns.EXPECT().Local().Return(conn).MaxTimes(1)

				ml.EXPECT().Members().Return([]membership.Member{
					{ID: 1, Name: "node1", Status: membership.StatusHealthy},
					{ID: 2, Name: "node2", Status: membership.StatusFaulty},
					{ID: 3, Name: "node3", Status: membership.StatusFaulty},
				})
			},
			req: &proto.PutRequest{
				Key:     "key",
				Value:   &proto.Value{Data: []byte("value")},
				Version: vclock.NewEncoded(vclock.V{1: 1}),
			},
			wantCode: codes.FailedPrecondition,
			wantErr:  errNotEnoughReplicas,
		},
		"WriteToLocalNodeFails": {
			skip:       true,
			writeLevel: consistency.One,
			setupCluster: func(ctrl *gomock.Controller, conns *replication.MockConnRegistry, ml *MockMemberlist) {
				self := membership.Member{
					ID:     1,
					Name:   "node1",
					Status: membership.StatusHealthy,
				}

				conn := nodeclient.NewMockConn(ctrl)
				conn.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)

				ml.EXPECT().SelfID().Return(membership.NodeID(1))
				ml.EXPECT().Members().Return([]membership.Member{self})

				conns.EXPECT().Local().Return(conn, nil)
			},
			req: &proto.PutRequest{
				Key:     "key",
				Version: vclock.NewEncoded(),
				Value:   &proto.Value{Data: []byte("value")},
			},
			wantCode: codes.Internal,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if test.skip {
				t.Skip("skipping test")
				return
			}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ml := NewMockMemberlist(ctrl)
			conns := replication.NewMockConnRegistry(ctrl)
			test.setupCluster(ctrl, conns, ml)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s := New(ml, conns, log.NewNopLogger(), consistency.One, test.writeLevel)
			got, err := s.ReplicatedPut(ctx, test.req)
			require.Equal(t, test.wantCode, status.Code(err), "wrong status code received: %d", err)
			require.Equal(t, test.want, got, "wrong response received")

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			}
		})
	}
}
