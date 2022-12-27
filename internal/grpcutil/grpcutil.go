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

// IsCanceled is a shortcut for checking if an error is a grpc canceled error.
// Note that it does not check for context.Canceled, only for gRPC canceled.
func IsCanceled(err error) bool {
	return ErrorCode(err) == codes.Canceled
}

// ErrorInfo extracts an error info from an error. If the error is not a gRPC
// error or does not contain an error info, it returns nil. In case of multiple
// error info, it returns the first one.
func ErrorInfo(err error) *errdetails.ErrorInfo {
	st := status.Convert(err)

	for _, detail := range st.Details() {
		if t, ok := detail.(*errdetails.ErrorInfo); ok && t != nil {
			return t
		}
	}

	return nil
}

// DebugInfo extracts a debug info from an error. If the error is not a gRPC
// error or does not contain a debug info, it returns nil. In case of multiple
// debug info, it returns the first one.
func DebugInfo(err error) *errdetails.DebugInfo {
	st := status.Convert(err)

	for _, detail := range st.Details() {
		if t, ok := detail.(*errdetails.DebugInfo); ok && t != nil {
			return t
		}
	}

	return nil
}

// RetryInfo extracts a retry info from an error. If the error is not a gRPC
// error or does not contain a retry info, it returns nil. In case of multiple
// retry info, it returns the first one.
func RetryInfo(err error) *errdetails.RetryInfo {
	st := status.Convert(err)

	for _, detail := range st.Details() {
		if t, ok := detail.(*errdetails.RetryInfo); ok && t != nil {
			return t
		}
	}

	return nil
}
