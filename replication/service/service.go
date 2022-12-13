package service

//go:generate moq -stub -out service_mock.go . cluster

import (
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/replication/consistency"
	"github.com/maxpoletaev/kv/replication/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

const (
	defaultReadTimeout      = time.Second * 5
	defaultWriteTimeout     = time.Second * 5
	defaultConsistencyLevel = consistency.Quorum
)

var (
	errLevelNotSatisfied = status.Error(codes.Unavailable, "unable to satisfy the desired consistency level")
	errNotEnoughReplicas = status.Error(codes.FailedPrecondition, "not enough replicas available to satisfy the consistency level")
	errMissingVersion    = status.Error(codes.InvalidArgument, "version is required")
	errMissingKey        = status.Error(codes.InvalidArgument, "key is required")
)

type nodePutResult struct {
	NodeID  membership.NodeID
	Version string
}

type nodeGetResult struct {
	NodeID membership.NodeID
	Values []*storagepb.VersionedValue
}

type nodeValue struct {
	NodeID membership.NodeID
	*storagepb.VersionedValue
}

type serviceOption func(s *ReplicationService)

func WithConsistencyLevel(read, write consistency.Level) serviceOption {
	return func(s *ReplicationService) {
		s.readLevel = read
		s.writeLevel = write
	}
}

type ReplicationService struct {
	proto.UnimplementedCoordinatorServiceServer

	cluster      Cluster
	logger       kitlog.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLevel    consistency.Level
	writeLevel   consistency.Level
}

func New(clust Cluster, logger kitlog.Logger, readLevel, writeLevel consistency.Level) *ReplicationService {
	return &ReplicationService{
		cluster:      clust,
		logger:       logger,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		readLevel:    readLevel,
		writeLevel:   writeLevel,
	}
}

func countAlive(members []membership.Member) (alive int) {
	for i := range members {
		if members[i].IsReacheable() {
			alive++
		}
	}

	return
}
