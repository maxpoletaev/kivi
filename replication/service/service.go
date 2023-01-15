package service

import (
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/replication"
	"github.com/maxpoletaev/kiwi/replication/consistency"
	"github.com/maxpoletaev/kiwi/replication/proto"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
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

	connections  replication.ConnRegistry
	members      Memberlist
	logger       kitlog.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLevel    consistency.Level
	writeLevel   consistency.Level
}

func New(
	members Memberlist,
	connections replication.ConnRegistry,
	logger kitlog.Logger,
	readLevel, writeLevel consistency.Level,
) *ReplicationService {
	return &ReplicationService{
		logger:       logger,
		members:      members,
		connections:  connections,
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
