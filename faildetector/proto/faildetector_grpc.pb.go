// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: faildetector/proto/faildetector.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// FailDetectorServiceClient is the client API for FailDetectorService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type FailDetectorServiceClient interface {
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
}

type failDetectorServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewFailDetectorServiceClient(cc grpc.ClientConnInterface) FailDetectorServiceClient {
	return &failDetectorServiceClient{cc}
}

func (c *failDetectorServiceClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, "/faildetector.FailDetectorService/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FailDetectorServiceServer is the server API for FailDetectorService service.
// All implementations must embed UnimplementedFailDetectorServiceServer
// for forward compatibility
type FailDetectorServiceServer interface {
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	mustEmbedUnimplementedFailDetectorServiceServer()
}

// UnimplementedFailDetectorServiceServer must be embedded to have forward compatible implementations.
type UnimplementedFailDetectorServiceServer struct {
}

func (UnimplementedFailDetectorServiceServer) Ping(context.Context, *PingRequest) (*PingResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedFailDetectorServiceServer) mustEmbedUnimplementedFailDetectorServiceServer() {}

// UnsafeFailDetectorServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to FailDetectorServiceServer will
// result in compilation errors.
type UnsafeFailDetectorServiceServer interface {
	mustEmbedUnimplementedFailDetectorServiceServer()
}

func RegisterFailDetectorServiceServer(s grpc.ServiceRegistrar, srv FailDetectorServiceServer) {
	s.RegisterService(&FailDetectorService_ServiceDesc, srv)
}

func _FailDetectorService_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FailDetectorServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/faildetector.FailDetectorService/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FailDetectorServiceServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// FailDetectorService_ServiceDesc is the grpc.ServiceDesc for FailDetectorService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var FailDetectorService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "faildetector.FailDetectorService",
	HandlerType: (*FailDetectorServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler:    _FailDetectorService_Ping_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "faildetector/proto/faildetector.proto",
}
