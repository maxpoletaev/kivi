package service

import (
	"context"
	"errors"
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/consistency"
	"github.com/maxpoletaev/kivi/replication/proto"
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

type ReplicationService struct {
	proto.UnimplementedReplicationServer

	cluster      membership.Cluster
	logger       kitlog.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLevel    consistency.Level
	writeLevel   consistency.Level
}

func New(cluster membership.Cluster, logger kitlog.Logger) *ReplicationService {
	return &ReplicationService{
		logger:       logger,
		cluster:      cluster,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		readLevel:    defaultConsistencyLevel,
		writeLevel:   defaultConsistencyLevel,
	}
}

func validateGetRequest(req *proto.GetRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if err := validateGetRequest(req); err != nil {
		return nil, err
	}

	get := replication.OpGet{
		Logger:  s.logger,
		Cluster: s.cluster,
		Level:   s.readLevel,
		Timeout: s.readTimeout,
	}

	result, err := get.Do(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	values := make([]string, 0, len(result.Values))
	for _, data := range result.Values {
		values = append(values, string(data))
	}

	return &proto.GetResponse{
		Version: result.Version,
		Values:  values,
	}, nil
}

func validatePutRequest(req *proto.PutRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if err := validatePutRequest(req); err != nil {
		return nil, err
	}

	put := &replication.OpPut{
		Cluster: s.cluster,
		Level:   s.writeLevel,
		Logger:  s.logger,
		Timeout: s.writeTimeout,
	}

	result, err := put.Do(ctx, req.Key, []byte(req.Value), req.Version)
	if err != nil {
		return nil, err
	}

	return &proto.PutResponse{
		Acknowledged: int32(result.Acknowledged),
		Version:      result.Version,
	}, nil
}

func validateDeleteRequest(req *proto.DeleteRequest) error {
	if req.Key == "" {
		return errMissingKey
	}

	if req.Version == "" {
		return errMissingVersion
	}

	return nil
}

func (s *ReplicationService) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if err := validateDeleteRequest(req); err != nil {
		return nil, err
	}

	del := &replication.OpDel{
		Cluster: s.cluster,
		Level:   s.writeLevel,
		Logger:  s.logger,
		Timeout: s.writeTimeout,
	}

	result, err := del.Do(ctx, req.Key, req.Version)
	if err != nil {
		if errors.Is(err, replication.ErrNotEnoughAcks) {
			return nil, errLevelNotSatisfied
		}

		return nil, err
	}

	return &proto.DeleteResponse{
		Acknowledged: int32(result.Acknowledged),
		Version:      result.Version,
	}, nil
}
