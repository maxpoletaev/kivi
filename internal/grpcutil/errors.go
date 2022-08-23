package grpcutil

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ErrorCode(err error) codes.Code {
	if st, ok := status.FromError(err); ok {
		return st.Code()
	}

	return codes.Unknown
}
