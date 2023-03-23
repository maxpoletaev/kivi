// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.19.4
// source: storage/lsmtree/proto/lsm.proto

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

type IndexEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key        string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	DataOffset int64  `protobuf:"varint,2,opt,name=data_offset,json=dataOffset,proto3" json:"data_offset,omitempty"`
}

func (x *IndexEntry) Reset() {
	*x = IndexEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IndexEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IndexEntry) ProtoMessage() {}

func (x *IndexEntry) ProtoReflect() protoreflect.Message {
	mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IndexEntry.ProtoReflect.Descriptor instead.
func (*IndexEntry) Descriptor() ([]byte, []int) {
	return file_storage_lsmtree_proto_lsm_proto_rawDescGZIP(), []int{0}
}

func (x *IndexEntry) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *IndexEntry) GetDataOffset() int64 {
	if x != nil {
		return x.DataOffset
	}
	return 0
}

type Value struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tombstone bool   `protobuf:"varint,1,opt,name=tombstone,proto3" json:"tombstone,omitempty"`
	Version   string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	Data      []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *Value) Reset() {
	*x = Value{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Value) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Value) ProtoMessage() {}

func (x *Value) ProtoReflect() protoreflect.Message {
	mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Value.ProtoReflect.Descriptor instead.
func (*Value) Descriptor() ([]byte, []int) {
	return file_storage_lsmtree_proto_lsm_proto_rawDescGZIP(), []int{1}
}

func (x *Value) GetTombstone() bool {
	if x != nil {
		return x.Tombstone
	}
	return false
}

func (x *Value) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *Value) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type DataEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key    string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Values []*Value `protobuf:"bytes,3,rep,name=values,proto3" json:"values,omitempty"`
}

func (x *DataEntry) Reset() {
	*x = DataEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DataEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DataEntry) ProtoMessage() {}

func (x *DataEntry) ProtoReflect() protoreflect.Message {
	mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DataEntry.ProtoReflect.Descriptor instead.
func (*DataEntry) Descriptor() ([]byte, []int) {
	return file_storage_lsmtree_proto_lsm_proto_rawDescGZIP(), []int{2}
}

func (x *DataEntry) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *DataEntry) GetValues() []*Value {
	if x != nil {
		return x.Values
	}
	return nil
}

type TableMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NumEntries int64 `protobuf:"varint,1,opt,name=num_entries,json=numEntries,proto3" json:"num_entries,omitempty"`
	Level      int32 `protobuf:"varint,2,opt,name=level,proto3" json:"level,omitempty"`
}

func (x *TableMeta) Reset() {
	*x = TableMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TableMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableMeta) ProtoMessage() {}

func (x *TableMeta) ProtoReflect() protoreflect.Message {
	mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableMeta.ProtoReflect.Descriptor instead.
func (*TableMeta) Descriptor() ([]byte, []int) {
	return file_storage_lsmtree_proto_lsm_proto_rawDescGZIP(), []int{3}
}

func (x *TableMeta) GetNumEntries() int64 {
	if x != nil {
		return x.NumEntries
	}
	return 0
}

func (x *TableMeta) GetLevel() int32 {
	if x != nil {
		return x.Level
	}
	return 0
}

type BloomFilter struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NumBytes  int32  `protobuf:"varint,1,opt,name=num_bytes,json=numBytes,proto3" json:"num_bytes,omitempty"`
	NumHashes int32  `protobuf:"varint,2,opt,name=num_hashes,json=numHashes,proto3" json:"num_hashes,omitempty"`
	Crc32     uint32 `protobuf:"varint,3,opt,name=crc32,proto3" json:"crc32,omitempty"`
	Data      []byte `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *BloomFilter) Reset() {
	*x = BloomFilter{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BloomFilter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BloomFilter) ProtoMessage() {}

func (x *BloomFilter) ProtoReflect() protoreflect.Message {
	mi := &file_storage_lsmtree_proto_lsm_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BloomFilter.ProtoReflect.Descriptor instead.
func (*BloomFilter) Descriptor() ([]byte, []int) {
	return file_storage_lsmtree_proto_lsm_proto_rawDescGZIP(), []int{4}
}

func (x *BloomFilter) GetNumBytes() int32 {
	if x != nil {
		return x.NumBytes
	}
	return 0
}

func (x *BloomFilter) GetNumHashes() int32 {
	if x != nil {
		return x.NumHashes
	}
	return 0
}

func (x *BloomFilter) GetCrc32() uint32 {
	if x != nil {
		return x.Crc32
	}
	return 0
}

func (x *BloomFilter) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_storage_lsmtree_proto_lsm_proto protoreflect.FileDescriptor

var file_storage_lsmtree_proto_lsm_proto_rawDesc = []byte{
	0x0a, 0x1f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f, 0x6c, 0x73, 0x6d, 0x74, 0x72, 0x65,
	0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x6c, 0x73, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x03, 0x6c, 0x73, 0x6d, 0x22, 0x3f, 0x0a, 0x0a, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1f, 0x0a, 0x0b, 0x64, 0x61, 0x74, 0x61, 0x5f, 0x6f,
	0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x64, 0x61, 0x74,
	0x61, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x53, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x12, 0x1c, 0x0a, 0x09, 0x74, 0x6f, 0x6d, 0x62, 0x73, 0x74, 0x6f, 0x6e, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x09, 0x74, 0x6f, 0x6d, 0x62, 0x73, 0x74, 0x6f, 0x6e, 0x65, 0x12, 0x18,
	0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x41, 0x0a, 0x09,
	0x44, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x22, 0x0a, 0x06, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x6c, 0x73,
	0x6d, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22,
	0x42, 0x0a, 0x09, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x1f, 0x0a, 0x0b,
	0x6e, 0x75, 0x6d, 0x5f, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x0a, 0x6e, 0x75, 0x6d, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x14, 0x0a,
	0x05, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x6c, 0x65,
	0x76, 0x65, 0x6c, 0x22, 0x73, 0x0a, 0x0b, 0x42, 0x6c, 0x6f, 0x6f, 0x6d, 0x46, 0x69, 0x6c, 0x74,
	0x65, 0x72, 0x12, 0x1b, 0x0a, 0x09, 0x6e, 0x75, 0x6d, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x6e, 0x75, 0x6d, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12,
	0x1d, 0x0a, 0x0a, 0x6e, 0x75, 0x6d, 0x5f, 0x68, 0x61, 0x73, 0x68, 0x65, 0x73, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x09, 0x6e, 0x75, 0x6d, 0x48, 0x61, 0x73, 0x68, 0x65, 0x73, 0x12, 0x14,
	0x0a, 0x05, 0x63, 0x72, 0x63, 0x33, 0x32, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x63,
	0x72, 0x63, 0x33, 0x32, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x2f, 0x5a, 0x2d, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6d, 0x61, 0x78, 0x70, 0x6f, 0x6c, 0x65, 0x74, 0x61,
	0x65, 0x76, 0x2f, 0x6b, 0x69, 0x76, 0x69, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f,
	0x6c, 0x73, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_storage_lsmtree_proto_lsm_proto_rawDescOnce sync.Once
	file_storage_lsmtree_proto_lsm_proto_rawDescData = file_storage_lsmtree_proto_lsm_proto_rawDesc
)

func file_storage_lsmtree_proto_lsm_proto_rawDescGZIP() []byte {
	file_storage_lsmtree_proto_lsm_proto_rawDescOnce.Do(func() {
		file_storage_lsmtree_proto_lsm_proto_rawDescData = protoimpl.X.CompressGZIP(file_storage_lsmtree_proto_lsm_proto_rawDescData)
	})
	return file_storage_lsmtree_proto_lsm_proto_rawDescData
}

var file_storage_lsmtree_proto_lsm_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_storage_lsmtree_proto_lsm_proto_goTypes = []interface{}{
	(*IndexEntry)(nil),  // 0: lsm.IndexEntry
	(*Value)(nil),       // 1: lsm.Value
	(*DataEntry)(nil),   // 2: lsm.DataEntry
	(*TableMeta)(nil),   // 3: lsm.TableMeta
	(*BloomFilter)(nil), // 4: lsm.BloomFilter
}
var file_storage_lsmtree_proto_lsm_proto_depIdxs = []int32{
	1, // 0: lsm.DataEntry.values:type_name -> lsm.Value
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_storage_lsmtree_proto_lsm_proto_init() }
func file_storage_lsmtree_proto_lsm_proto_init() {
	if File_storage_lsmtree_proto_lsm_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_storage_lsmtree_proto_lsm_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IndexEntry); i {
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
		file_storage_lsmtree_proto_lsm_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Value); i {
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
		file_storage_lsmtree_proto_lsm_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DataEntry); i {
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
		file_storage_lsmtree_proto_lsm_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TableMeta); i {
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
		file_storage_lsmtree_proto_lsm_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BloomFilter); i {
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
			RawDescriptor: file_storage_lsmtree_proto_lsm_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_storage_lsmtree_proto_lsm_proto_goTypes,
		DependencyIndexes: file_storage_lsmtree_proto_lsm_proto_depIdxs,
		MessageInfos:      file_storage_lsmtree_proto_lsm_proto_msgTypes,
	}.Build()
	File_storage_lsmtree_proto_lsm_proto = out.File
	file_storage_lsmtree_proto_lsm_proto_rawDesc = nil
	file_storage_lsmtree_proto_lsm_proto_goTypes = nil
	file_storage_lsmtree_proto_lsm_proto_depIdxs = nil
}
