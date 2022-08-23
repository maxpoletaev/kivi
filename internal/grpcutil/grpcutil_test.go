package grpcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorCode(t *testing.T) {
	err := status.New(codes.DataLoss, "").Err()

	assert.Equal(t, codes.DataLoss, ErrorCode(err))
	assert.Equal(t, codes.Unknown, ErrorCode(assert.AnError))
}
