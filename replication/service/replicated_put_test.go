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

	clustmock "github.com/maxpoletaev/kv/clust/mock"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/replication/consistency"
	"github.com/maxpoletaev/kv/replication/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

func TestReplicatedPut(t *testing.T) {
	tests := map[string]struct {
		setupCluster func(ctrl *gomock.Controller, conns *MockConnRegistry, ml *MockMemberlist)
		writeLevel   consistency.Level
		req          *proto.PutRequest
		want         *proto.PutResponse
		wantCode     codes.Code
		wantErr      error
	}{
		"OneOfThreeNodesInQuorumFails": {
			writeLevel: consistency.Quorum,
			setupCluster: func(ctrl *gomock.Controller, conns *MockConnRegistry, ml *MockMemberlist) {
				conn1 := clustmock.NewMockClient(ctrl)
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

				conn2 := clustmock.NewMockClient(ctrl)
				conn2.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(&storagepb.PutResponse{
					Version: vclock.NewEncoded(vclock.V{1: 1}),
				}, nil).MaxTimes(1)

				conn3 := clustmock.NewMockClient(ctrl)
				conn3.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(nil, assert.AnError).MaxTimes(1)

				members := []membership.Member{
					{ID: 1, Name: "node1", Status: membership.StatusHealthy},
					{ID: 2, Name: "node2", Status: membership.StatusHealthy},
					{ID: 3, Name: "node3", Status: membership.StatusHealthy},
				}

				ml.EXPECT().Self().Return(members[0])
				ml.EXPECT().Members().Return(members)

				conns.EXPECT().Get(membership.NodeID(1)).Return(conn1, nil).MaxTimes(1)
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
			setupCluster: func(ctrl *gomock.Controller, conns *MockConnRegistry, ml *MockMemberlist) {
				conn1 := clustmock.NewMockClient(ctrl)
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

				conn2 := clustmock.NewMockClient(ctrl)
				conn2.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(nil, assert.AnError).MaxTimes(1)

				conn3 := clustmock.NewMockClient(ctrl)
				conn3.EXPECT().Put(gomock.Any(), &storagepb.PutRequest{
					Key: "key",
					Value: &storagepb.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}).Return(nil, assert.AnError).MaxTimes(1)

				members := []membership.Member{
					{ID: 1, Name: "node1", Status: membership.StatusHealthy},
					{ID: 2, Name: "node2", Status: membership.StatusHealthy},
					{ID: 3, Name: "node3", Status: membership.StatusHealthy},
				}

				ml.EXPECT().Self().Return(members[0])
				ml.EXPECT().Members().Return(members)

				conns.EXPECT().Get(membership.NodeID(1)).Return(conn1, nil).MaxTimes(1)
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
			setupCluster: func(ctrl *gomock.Controller, conns *MockConnRegistry, ml *MockMemberlist) {
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
			writeLevel: consistency.One,
			setupCluster: func(ctrl *gomock.Controller, conns *MockConnRegistry, ml *MockMemberlist) {
				self := membership.Member{
					ID:     1,
					Name:   "node1",
					Status: membership.StatusHealthy,
				}

				conn := clustmock.NewMockClient(ctrl)
				conn.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)

				ml.EXPECT().Self().Return(self)
				ml.EXPECT().Members().Return([]membership.Member{self})

				conns.EXPECT().Get(self.ID).Return(conn, nil)
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
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ml := NewMockMemberlist(ctrl)
			conns := NewMockConnRegistry(ctrl)
			test.setupCluster(ctrl, conns, ml)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s := New(ml, conns, log.NewNopLogger(), consistency.One, test.writeLevel)
			got, err := s.ReplicatedPut(ctx, test.req)
			require.Equal(t, test.wantCode, status.Code(err), err)
			require.Equal(t, test.want, got)

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			}
		})
	}
}
