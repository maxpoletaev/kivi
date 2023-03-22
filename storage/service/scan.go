package service

import (
	"errors"

	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/proto"
)

func (s *StorageService) Scan(req *proto.ScanRequest, stream proto.StorageService_ScanServer) error {
	st, ok := s.storage.(storage.Scannable)
	if !ok {
		return errNotSupported
	}

	it := st.Scan(req.StartKey)

	for {
		if err := it.Next(); err != nil {
			if errors.Is(err, storage.ErrNoMoreItems) {
				return nil
			}

			return err
		}

		key, values := it.Item()
		resp := &proto.ScanResponse{
			Value: toProtoValues(values),
			Key:   key,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
