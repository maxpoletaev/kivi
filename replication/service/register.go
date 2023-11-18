package service

import (
	"context"

	"github.com/maxpoletaev/kivi/replication/datatypes"
	"github.com/maxpoletaev/kivi/replication/proto"
)

func formatRegisterKey(key string) string {
	return key + ".lww"
}

func (s *ReplicationService) RegisterGet(ctx context.Context, req *proto.RegisterGetRequest) (*proto.RegisterGetResponse, error) {
	get := typeMutation[*datatypes.Register]{
		New:     datatypes.NewRegister,
		cluster: s.cluster,
		timeout: s.readTimeout,
		level:   s.readLevel,
		logger:  s.logger,
	}

	res, err := get.Do(
		ctx,
		formatRegisterKey(req.Key),
		func(reg *datatypes.Register) error {
			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &proto.RegisterGetResponse{
		Value: res.value.Get(),
	}, nil
}

func (s *ReplicationService) RegisterPut(ctx context.Context, req *proto.RegisterPutRequest) (*proto.RegisterPutResponse, error) {
	mutation := typeMutation[*datatypes.Register]{
		New:     datatypes.NewRegister,
		cluster: s.cluster,
		timeout: s.writeTimeout,
		level:   s.writeLevel,
		logger:  s.logger,
	}

	res, err := mutation.DoWithConflictRetry(
		ctx,
		formatRegisterKey(req.Key),
		func(reg *datatypes.Register) error {
			reg.Put(req.Value)
			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &proto.RegisterPutResponse{
		Acknowledged: int32(res.acknowledged),
	}, nil
}
