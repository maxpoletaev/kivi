package service

import (
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/replication/consistency"
	"github.com/maxpoletaev/kv/replication/proto"
	spb "github.com/maxpoletaev/kv/storage/proto"
)

const (
	defaultReadTimeout      = time.Second * 5
	defaultWriteTimeout     = time.Second * 5
	defaultConsistencyLevel = consistency.LevelQuorum
)

var (
	errLevelNotSatisfied = status.Error(codes.Unavailable, "unable to satisfy the desired consistency level")
	errNotEnoughReplicas = status.Error(codes.FailedPrecondition, "not enough replicas available to satisfy the consistency level")
)

type cluster interface {
	LocalNode() clustering.Node
	Nodes() []clustering.Node
}

type replicaPutResult struct {
	Version     vclock.Vector
	ReplicaName string
}

type replicaGetResult struct {
	Values      []*spb.VersionedValue
	ReplicaName string
}

type replicaValue struct {
	*spb.VersionedValue
	ReplicaName string
}

type CoordinatorService struct {
	proto.UnimplementedCoordinatorServiceServer

	cluster          cluster
	logger           kitlog.Logger
	readTimeout      time.Duration
	writeTimeout     time.Duration
	readConsistency  consistency.Level
	writeConsistency consistency.Level
}

func New(clust cluster, logger kitlog.Logger) *CoordinatorService {
	return &CoordinatorService{
		cluster:          clust,
		logger:           logger,
		readTimeout:      defaultReadTimeout,
		writeTimeout:     defaultWriteTimeout,
		readConsistency:  defaultConsistencyLevel,
		writeConsistency: defaultConsistencyLevel,
	}
}
