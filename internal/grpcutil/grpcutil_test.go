package grpcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorCode(t *testing.T) {
	err := status.New(codes.DataLoss, "").Err()

	assert.Equal(t, codes.DataLoss, ErrorCode(err))
	assert.Equal(t, codes.Unknown, ErrorCode(assert.AnError))
}

func TestErrorInfo(t *testing.T) {
	st, _ := status.New(codes.DataLoss, "data loss").WithDetails(&errdetails.ErrorInfo{
		Domain: "domain",
		Reason: "reason",
	})

	info := ErrorInfo(st.Err())
	assert.Equal(t, "domain", info.Domain)
	assert.Equal(t, "reason", info.Reason)
}

func TestErrorInfo_NoErrorInfo(t *testing.T) {
	err := status.New(codes.DataLoss, "").Err()
	assert.Nil(t, ErrorInfo(err))
}

func TestErrorInfo_NotGrpcError(t *testing.T) {
	assert.Nil(t, ErrorInfo(assert.AnError))
}
