package service

import (
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeapi"
	"github.com/maxpoletaev/kiwi/replication"
	"github.com/maxpoletaev/kiwi/replication/consistency"
	"github.com/maxpoletaev/kiwi/replication/proto"
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
	nodeapi.VersionedValue
}

type serviceOption func(s *ReplicationServer)

func WithConsistencyLevel(read, write consistency.Level) serviceOption {
	return func(s *ReplicationServer) {
		s.readLevel = read
		s.writeLevel = write
	}
}

type ReplicationServer struct {
	proto.UnimplementedReplicationServer

	cluster      replication.Cluster
	logger       kitlog.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLevel    consistency.Level
	writeLevel   consistency.Level
}

func New(cluster replication.Cluster, logger kitlog.Logger) *ReplicationServer {
	return &ReplicationServer{
		logger:       logger,
		cluster:      cluster,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		readLevel:    defaultConsistencyLevel,
		writeLevel:   defaultConsistencyLevel,
	}
}

func countAlive(members []membership.Node) (alive int) {
	for i := range members {
		if members[i].IsReachable() {
			alive++
		}
	}

	return
}
