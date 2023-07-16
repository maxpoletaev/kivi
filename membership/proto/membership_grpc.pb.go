// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: membership/proto/membership.proto

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

// MembershipClient is the client API for Membership service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MembershipClient interface {
	ListNodes(ctx context.Context, in *ListNodesRequest, opts ...grpc.CallOption) (*ListNodesResponse, error)
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
	PullPushState(ctx context.Context, in *PullPushStateRequest, opts ...grpc.CallOption) (*PullPushStateResponse, error)
	PingIndirect(ctx context.Context, in *PingIndirectRequest, opts ...grpc.CallOption) (*PingIndirectResponse, error)
}

type membershipClient struct {
	cc grpc.ClientConnInterface
}

func NewMembershipClient(cc grpc.ClientConnInterface) MembershipClient {
	return &membershipClient{cc}
}

func (c *membershipClient) ListNodes(ctx context.Context, in *ListNodesRequest, opts ...grpc.CallOption) (*ListNodesResponse, error) {
	out := new(ListNodesResponse)
	err := c.cc.Invoke(ctx, "/membership.Membership/ListNodes", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *membershipClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, "/membership.Membership/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *membershipClient) PullPushState(ctx context.Context, in *PullPushStateRequest, opts ...grpc.CallOption) (*PullPushStateResponse, error) {
	out := new(PullPushStateResponse)
	err := c.cc.Invoke(ctx, "/membership.Membership/PullPushState", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *membershipClient) PingIndirect(ctx context.Context, in *PingIndirectRequest, opts ...grpc.CallOption) (*PingIndirectResponse, error) {
	out := new(PingIndirectResponse)
	err := c.cc.Invoke(ctx, "/membership.Membership/PingIndirect", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MembershipServer is the server API for Membership service.
// All implementations must embed UnimplementedMembershipServer
// for forward compatibility
type MembershipServer interface {
	ListNodes(context.Context, *ListNodesRequest) (*ListNodesResponse, error)
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	PullPushState(context.Context, *PullPushStateRequest) (*PullPushStateResponse, error)
	PingIndirect(context.Context, *PingIndirectRequest) (*PingIndirectResponse, error)
	mustEmbedUnimplementedMembershipServer()
}

// UnimplementedMembershipServer must be embedded to have forward compatible implementations.
type UnimplementedMembershipServer struct {
}

func (UnimplementedMembershipServer) ListNodes(context.Context, *ListNodesRequest) (*ListNodesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListNodes not implemented")
}
func (UnimplementedMembershipServer) Ping(context.Context, *PingRequest) (*PingResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedMembershipServer) PullPushState(context.Context, *PullPushStateRequest) (*PullPushStateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PullPushState not implemented")
}
func (UnimplementedMembershipServer) PingIndirect(context.Context, *PingIndirectRequest) (*PingIndirectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PingIndirect not implemented")
}
func (UnimplementedMembershipServer) mustEmbedUnimplementedMembershipServer() {}

// UnsafeMembershipServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MembershipServer will
// result in compilation errors.
type UnsafeMembershipServer interface {
	mustEmbedUnimplementedMembershipServer()
}

func RegisterMembershipServer(s grpc.ServiceRegistrar, srv MembershipServer) {
	s.RegisterService(&Membership_ServiceDesc, srv)
}

func _Membership_ListNodes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListNodesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MembershipServer).ListNodes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/membership.Membership/ListNodes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MembershipServer).ListNodes(ctx, req.(*ListNodesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Membership_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MembershipServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/membership.Membership/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MembershipServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Membership_PullPushState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PullPushStateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MembershipServer).PullPushState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/membership.Membership/PullPushState",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MembershipServer).PullPushState(ctx, req.(*PullPushStateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Membership_PingIndirect_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingIndirectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MembershipServer).PingIndirect(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/membership.Membership/PingIndirect",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MembershipServer).PingIndirect(ctx, req.(*PingIndirectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Membership_ServiceDesc is the grpc.ServiceDesc for Membership service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Membership_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "membership.Membership",
	HandlerType: (*MembershipServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ListNodes",
			Handler:    _Membership_ListNodes_Handler,
		},
		{
			MethodName: "Ping",
			Handler:    _Membership_Ping_Handler,
		},
		{
			MethodName: "PullPushState",
			Handler:    _Membership_PullPushState_Handler,
		},
		{
			MethodName: "PingIndirect",
			Handler:    _Membership_PingIndirect_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "membership/proto/membership.proto",
}
