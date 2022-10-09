// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: replication/proto/replication.proto

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

// CoordinatorServiceClient is the client API for CoordinatorService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type CoordinatorServiceClient interface {
	ClusterInfo(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*ClusterInfoResponse, error)
	ReplicatedGet(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	ReplicatedPut(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
}

type coordinatorServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewCoordinatorServiceClient(cc grpc.ClientConnInterface) CoordinatorServiceClient {
	return &coordinatorServiceClient{cc}
}

func (c *coordinatorServiceClient) ClusterInfo(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*ClusterInfoResponse, error) {
	out := new(ClusterInfoResponse)
	err := c.cc.Invoke(ctx, "/replication.CoordinatorService/ClusterInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorServiceClient) ReplicatedGet(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/replication.CoordinatorService/ReplicatedGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorServiceClient) ReplicatedPut(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/replication.CoordinatorService/ReplicatedPut", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CoordinatorServiceServer is the server API for CoordinatorService service.
// All implementations must embed UnimplementedCoordinatorServiceServer
// for forward compatibility
type CoordinatorServiceServer interface {
	ClusterInfo(context.Context, *Empty) (*ClusterInfoResponse, error)
	ReplicatedGet(context.Context, *GetRequest) (*GetResponse, error)
	ReplicatedPut(context.Context, *PutRequest) (*PutResponse, error)
	mustEmbedUnimplementedCoordinatorServiceServer()
}

// UnimplementedCoordinatorServiceServer must be embedded to have forward compatible implementations.
type UnimplementedCoordinatorServiceServer struct {
}

func (UnimplementedCoordinatorServiceServer) ClusterInfo(context.Context, *Empty) (*ClusterInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClusterInfo not implemented")
}
func (UnimplementedCoordinatorServiceServer) ReplicatedGet(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicatedGet not implemented")
}
func (UnimplementedCoordinatorServiceServer) ReplicatedPut(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicatedPut not implemented")
}
func (UnimplementedCoordinatorServiceServer) mustEmbedUnimplementedCoordinatorServiceServer() {}

// UnsafeCoordinatorServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CoordinatorServiceServer will
// result in compilation errors.
type UnsafeCoordinatorServiceServer interface {
	mustEmbedUnimplementedCoordinatorServiceServer()
}

func RegisterCoordinatorServiceServer(s grpc.ServiceRegistrar, srv CoordinatorServiceServer) {
	s.RegisterService(&CoordinatorService_ServiceDesc, srv)
}

func _CoordinatorService_ClusterInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).ClusterInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/replication.CoordinatorService/ClusterInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorServiceServer).ClusterInfo(ctx, req.(*Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorService_ReplicatedGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).ReplicatedGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/replication.CoordinatorService/ReplicatedGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorServiceServer).ReplicatedGet(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorService_ReplicatedPut_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceServer).ReplicatedPut(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/replication.CoordinatorService/ReplicatedPut",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorServiceServer).ReplicatedPut(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// CoordinatorService_ServiceDesc is the grpc.ServiceDesc for CoordinatorService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var CoordinatorService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "replication.CoordinatorService",
	HandlerType: (*CoordinatorServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ClusterInfo",
			Handler:    _CoordinatorService_ClusterInfo_Handler,
		},
		{
			MethodName: "ReplicatedGet",
			Handler:    _CoordinatorService_ReplicatedGet_Handler,
		},
		{
			MethodName: "ReplicatedPut",
			Handler:    _CoordinatorService_ReplicatedPut_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "replication/proto/replication.proto",
}
