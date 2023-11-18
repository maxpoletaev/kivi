// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v4.24.4
// source: membership/proto/membership.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Status int32

const (
	Status_HEALTHY   Status = 0
	Status_UNHEALTHY Status = 1
	Status_LEFT      Status = 2
)

// Enum value maps for Status.
var (
	Status_name = map[int32]string{
		0: "HEALTHY",
		1: "UNHEALTHY",
		2: "LEFT",
	}
	Status_value = map[string]int32{
		"HEALTHY":   0,
		"UNHEALTHY": 1,
		"LEFT":      2,
	}
)

func (x Status) Enum() *Status {
	p := new(Status)
	*p = x
	return p
}

func (x Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Status) Descriptor() protoreflect.EnumDescriptor {
	return file_membership_proto_membership_proto_enumTypes[0].Descriptor()
}

func (Status) Type() protoreflect.EnumType {
	return &file_membership_proto_membership_proto_enumTypes[0]
}

func (x Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Status.Descriptor instead.
func (Status) EnumDescriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{0}
}

type Node struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id         uint32 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Name       string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Address    string `protobuf:"bytes,3,opt,name=address,proto3" json:"address,omitempty"`
	Generation uint32 `protobuf:"varint,4,opt,name=generation,proto3" json:"generation,omitempty"`
	Status     Status `protobuf:"varint,5,opt,name=status,proto3,enum=membership.Status" json:"status,omitempty"`
	Error      string `protobuf:"bytes,6,opt,name=error,proto3" json:"error,omitempty"`
	RunId      int64  `protobuf:"varint,7,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
}

func (x *Node) Reset() {
	*x = Node{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Node) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Node) ProtoMessage() {}

func (x *Node) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Node.ProtoReflect.Descriptor instead.
func (*Node) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{0}
}

func (x *Node) GetId() uint32 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Node) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Node) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

func (x *Node) GetGeneration() uint32 {
	if x != nil {
		return x.Generation
	}
	return 0
}

func (x *Node) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_HEALTHY
}

func (x *Node) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *Node) GetRunId() int64 {
	if x != nil {
		return x.RunId
	}
	return 0
}

type ListNodesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ListNodesRequest) Reset() {
	*x = ListNodesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListNodesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListNodesRequest) ProtoMessage() {}

func (x *ListNodesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListNodesRequest.ProtoReflect.Descriptor instead.
func (*ListNodesRequest) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{1}
}

type ListNodesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Nodes []*Node `protobuf:"bytes,1,rep,name=nodes,proto3" json:"nodes,omitempty"`
}

func (x *ListNodesResponse) Reset() {
	*x = ListNodesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListNodesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListNodesResponse) ProtoMessage() {}

func (x *ListNodesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListNodesResponse.ProtoReflect.Descriptor instead.
func (*ListNodesResponse) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{2}
}

func (x *ListNodesResponse) GetNodes() []*Node {
	if x != nil {
		return x.Nodes
	}
	return nil
}

type PullPushStateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NodeId uint32  `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Nodes  []*Node `protobuf:"bytes,2,rep,name=nodes,proto3" json:"nodes,omitempty"`
}

func (x *PullPushStateRequest) Reset() {
	*x = PullPushStateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PullPushStateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PullPushStateRequest) ProtoMessage() {}

func (x *PullPushStateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PullPushStateRequest.ProtoReflect.Descriptor instead.
func (*PullPushStateRequest) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{3}
}

func (x *PullPushStateRequest) GetNodeId() uint32 {
	if x != nil {
		return x.NodeId
	}
	return 0
}

func (x *PullPushStateRequest) GetNodes() []*Node {
	if x != nil {
		return x.Nodes
	}
	return nil
}

type PullPushStateResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Nodes []*Node `protobuf:"bytes,1,rep,name=nodes,proto3" json:"nodes,omitempty"`
}

func (x *PullPushStateResponse) Reset() {
	*x = PullPushStateResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PullPushStateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PullPushStateResponse) ProtoMessage() {}

func (x *PullPushStateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PullPushStateResponse.ProtoReflect.Descriptor instead.
func (*PullPushStateResponse) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{4}
}

func (x *PullPushStateResponse) GetNodes() []*Node {
	if x != nil {
		return x.Nodes
	}
	return nil
}

type PingRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *PingRequest) Reset() {
	*x = PingRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PingRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingRequest) ProtoMessage() {}

func (x *PingRequest) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PingRequest.ProtoReflect.Descriptor instead.
func (*PingRequest) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{5}
}

type PingResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StateHash uint64 `protobuf:"varint,1,opt,name=state_hash,json=stateHash,proto3" json:"state_hash,omitempty"`
}

func (x *PingResponse) Reset() {
	*x = PingResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PingResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingResponse) ProtoMessage() {}

func (x *PingResponse) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PingResponse.ProtoReflect.Descriptor instead.
func (*PingResponse) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{6}
}

func (x *PingResponse) GetStateHash() uint64 {
	if x != nil {
		return x.StateHash
	}
	return 0
}

type PingIndirectRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NodeId  uint32 `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Timeout int64  `protobuf:"varint,2,opt,name=timeout,proto3" json:"timeout,omitempty"`
}

func (x *PingIndirectRequest) Reset() {
	*x = PingIndirectRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PingIndirectRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingIndirectRequest) ProtoMessage() {}

func (x *PingIndirectRequest) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PingIndirectRequest.ProtoReflect.Descriptor instead.
func (*PingIndirectRequest) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{7}
}

func (x *PingIndirectRequest) GetNodeId() uint32 {
	if x != nil {
		return x.NodeId
	}
	return 0
}

func (x *PingIndirectRequest) GetTimeout() int64 {
	if x != nil {
		return x.Timeout
	}
	return 0
}

type PingIndirectResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Duration int64  `protobuf:"varint,1,opt,name=duration,proto3" json:"duration,omitempty"`
	Status   Status `protobuf:"varint,2,opt,name=status,proto3,enum=membership.Status" json:"status,omitempty"`
	Message  string `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *PingIndirectResponse) Reset() {
	*x = PingIndirectResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_membership_proto_membership_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PingIndirectResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingIndirectResponse) ProtoMessage() {}

func (x *PingIndirectResponse) ProtoReflect() protoreflect.Message {
	mi := &file_membership_proto_membership_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PingIndirectResponse.ProtoReflect.Descriptor instead.
func (*PingIndirectResponse) Descriptor() ([]byte, []int) {
	return file_membership_proto_membership_proto_rawDescGZIP(), []int{8}
}

func (x *PingIndirectResponse) GetDuration() int64 {
	if x != nil {
		return x.Duration
	}
	return 0
}

func (x *PingIndirectResponse) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_HEALTHY
}

func (x *PingIndirectResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_membership_proto_membership_proto protoreflect.FileDescriptor

var file_membership_proto_membership_proto_rawDesc = []byte{
	0x0a, 0x21, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2f, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0a, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x22,
	0xbd, 0x01, 0x0a, 0x04, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07,
	0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61,
	0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0a, 0x67, 0x65, 0x6e, 0x65,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x2a, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73,
	0x68, 0x69, 0x70, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x15, 0x0a, 0x06, 0x72, 0x75, 0x6e, 0x5f,
	0x69, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x72, 0x75, 0x6e, 0x49, 0x64, 0x22,
	0x12, 0x0a, 0x10, 0x4c, 0x69, 0x73, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x22, 0x3b, 0x0a, 0x11, 0x4c, 0x69, 0x73, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x26, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72,
	0x73, 0x68, 0x69, 0x70, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73,
	0x22, 0x57, 0x0a, 0x14, 0x50, 0x75, 0x6c, 0x6c, 0x50, 0x75, 0x73, 0x68, 0x53, 0x74, 0x61, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x6e, 0x6f, 0x64, 0x65,
	0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x6e, 0x6f, 0x64, 0x65, 0x49,
	0x64, 0x12, 0x26, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x10, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x4e, 0x6f,
	0x64, 0x65, 0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x22, 0x3f, 0x0a, 0x15, 0x50, 0x75, 0x6c,
	0x6c, 0x50, 0x75, 0x73, 0x68, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x26, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x10, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x4e,
	0x6f, 0x64, 0x65, 0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x22, 0x0d, 0x0a, 0x0b, 0x50, 0x69,
	0x6e, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x2d, 0x0a, 0x0c, 0x50, 0x69, 0x6e,
	0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x74, 0x61,
	0x74, 0x65, 0x5f, 0x68, 0x61, 0x73, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x73,
	0x74, 0x61, 0x74, 0x65, 0x48, 0x61, 0x73, 0x68, 0x22, 0x48, 0x0a, 0x13, 0x50, 0x69, 0x6e, 0x67,
	0x49, 0x6e, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x17, 0x0a, 0x07, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x06, 0x6e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x74, 0x69, 0x6d, 0x65,
	0x6f, 0x75, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f,
	0x75, 0x74, 0x22, 0x78, 0x0a, 0x14, 0x50, 0x69, 0x6e, 0x67, 0x49, 0x6e, 0x64, 0x69, 0x72, 0x65,
	0x63, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x64, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x2a, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73,
	0x68, 0x69, 0x70, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2a, 0x2e, 0x0a, 0x06,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x0b, 0x0a, 0x07, 0x48, 0x45, 0x41, 0x4c, 0x54, 0x48,
	0x59, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x55, 0x4e, 0x48, 0x45, 0x41, 0x4c, 0x54, 0x48, 0x59,
	0x10, 0x01, 0x12, 0x08, 0x0a, 0x04, 0x4c, 0x45, 0x46, 0x54, 0x10, 0x02, 0x32, 0xc2, 0x02, 0x0a,
	0x0a, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x12, 0x4a, 0x0a, 0x09, 0x4c,
	0x69, 0x73, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x73, 0x12, 0x1c, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65,
	0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x73, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73,
	0x68, 0x69, 0x70, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x3b, 0x0a, 0x04, 0x50, 0x69, 0x6e, 0x67, 0x12,
	0x17, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x50, 0x69, 0x6e,
	0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x18, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65,
	0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x50, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x00, 0x12, 0x56, 0x0a, 0x0d, 0x50, 0x75, 0x6c, 0x6c, 0x50, 0x75, 0x73, 0x68,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x20, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68,
	0x69, 0x70, 0x2e, 0x50, 0x75, 0x6c, 0x6c, 0x50, 0x75, 0x73, 0x68, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x21, 0x2e, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72,
	0x73, 0x68, 0x69, 0x70, 0x2e, 0x50, 0x75, 0x6c, 0x6c, 0x50, 0x75, 0x73, 0x68, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x53, 0x0a, 0x0c,
	0x50, 0x69, 0x6e, 0x67, 0x49, 0x6e, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x12, 0x1f, 0x2e, 0x6d,
	0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x50, 0x69, 0x6e, 0x67, 0x49, 0x6e,
	0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e,
	0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2e, 0x50, 0x69, 0x6e, 0x67, 0x49,
	0x6e, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x42, 0x2e, 0x5a, 0x2c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x6d, 0x61, 0x78, 0x70, 0x6f, 0x6c, 0x65, 0x74, 0x61, 0x65, 0x76, 0x2f, 0x6b, 0x69, 0x76, 0x69,
	0x2f, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x68, 0x69, 0x70, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_membership_proto_membership_proto_rawDescOnce sync.Once
	file_membership_proto_membership_proto_rawDescData = file_membership_proto_membership_proto_rawDesc
)

func file_membership_proto_membership_proto_rawDescGZIP() []byte {
	file_membership_proto_membership_proto_rawDescOnce.Do(func() {
		file_membership_proto_membership_proto_rawDescData = protoimpl.X.CompressGZIP(file_membership_proto_membership_proto_rawDescData)
	})
	return file_membership_proto_membership_proto_rawDescData
}

var file_membership_proto_membership_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_membership_proto_membership_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_membership_proto_membership_proto_goTypes = []interface{}{
	(Status)(0),                   // 0: membership.Status
	(*Node)(nil),                  // 1: membership.Node
	(*ListNodesRequest)(nil),      // 2: membership.ListNodesRequest
	(*ListNodesResponse)(nil),     // 3: membership.ListNodesResponse
	(*PullPushStateRequest)(nil),  // 4: membership.PullPushStateRequest
	(*PullPushStateResponse)(nil), // 5: membership.PullPushStateResponse
	(*PingRequest)(nil),           // 6: membership.PingRequest
	(*PingResponse)(nil),          // 7: membership.PingResponse
	(*PingIndirectRequest)(nil),   // 8: membership.PingIndirectRequest
	(*PingIndirectResponse)(nil),  // 9: membership.PingIndirectResponse
}
var file_membership_proto_membership_proto_depIdxs = []int32{
	0, // 0: membership.Node.status:type_name -> membership.Status
	1, // 1: membership.ListNodesResponse.nodes:type_name -> membership.Node
	1, // 2: membership.PullPushStateRequest.nodes:type_name -> membership.Node
	1, // 3: membership.PullPushStateResponse.nodes:type_name -> membership.Node
	0, // 4: membership.PingIndirectResponse.status:type_name -> membership.Status
	2, // 5: membership.Membership.ListNodes:input_type -> membership.ListNodesRequest
	6, // 6: membership.Membership.Ping:input_type -> membership.PingRequest
	4, // 7: membership.Membership.PullPushState:input_type -> membership.PullPushStateRequest
	8, // 8: membership.Membership.PingIndirect:input_type -> membership.PingIndirectRequest
	3, // 9: membership.Membership.ListNodes:output_type -> membership.ListNodesResponse
	7, // 10: membership.Membership.Ping:output_type -> membership.PingResponse
	5, // 11: membership.Membership.PullPushState:output_type -> membership.PullPushStateResponse
	9, // 12: membership.Membership.PingIndirect:output_type -> membership.PingIndirectResponse
	9, // [9:13] is the sub-list for method output_type
	5, // [5:9] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_membership_proto_membership_proto_init() }
func file_membership_proto_membership_proto_init() {
	if File_membership_proto_membership_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_membership_proto_membership_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Node); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListNodesRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListNodesResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PullPushStateRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PullPushStateResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PingRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PingResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PingIndirectRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_membership_proto_membership_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PingIndirectResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_membership_proto_membership_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_membership_proto_membership_proto_goTypes,
		DependencyIndexes: file_membership_proto_membership_proto_depIdxs,
		EnumInfos:         file_membership_proto_membership_proto_enumTypes,
		MessageInfos:      file_membership_proto_membership_proto_msgTypes,
	}.Build()
	File_membership_proto_membership_proto = out.File
	file_membership_proto_membership_proto_rawDesc = nil
	file_membership_proto_membership_proto_goTypes = nil
	file_membership_proto_membership_proto_depIdxs = nil
}
