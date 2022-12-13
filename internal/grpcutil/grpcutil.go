package grpcutil

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode extracts a gRPC error code from an error. If the error is not a
// gRPC error, it returns codes.Unknown.
func ErrorCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	if st, ok := status.FromError(err); ok {
		return st.Code()
	}

	return codes.Unknown
}

func IsCanceled(err error) bool {
	return ErrorCode(err) == codes.Canceled
}

// ErrorInfo extracts an error info from an error. If the error is not a gRPC
// error or does not contain an error info, it returns nil.
func ErrorInfo(err error) *errdetails.ErrorInfo {
	st := status.Convert(err)

	for _, detail := range st.Details() {
		switch t := detail.(type) {
		case *errdetails.ErrorInfo:
			return t

		}
	}

	return nil
}
