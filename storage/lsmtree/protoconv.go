package lsmtree

import "github.com/maxpoletaev/kiwi/storage/lsmtree/proto"

func fromProtoMemtableInfo(info *proto.MemtableInfo) *MemtableInfo {
	return &MemtableInfo{
		ID:      info.Id,
		WALFile: info.WalFile,
	}
}

func toProtoMemtableInfo(info *MemtableInfo) *proto.MemtableInfo {
	return &proto.MemtableInfo{
		Id:      info.ID,
		WalFile: info.WALFile,
	}
}

func fromProtoSSTableInfo(info *proto.SSTableInfo) *SSTableInfo {
	return &SSTableInfo{
		ID:         info.Id,
		Level:      int(info.Level),
		NumEntries: info.NumRecords,
		Size:       info.Size,
		IndexFile:  info.IndexFile,
		DataFile:   info.DataFile,
		BloomFile:  info.BloomFile,
	}
}

func toProtoSSTableInfo(info *SSTableInfo) *proto.SSTableInfo {
	return &proto.SSTableInfo{
		Id:         info.ID,
		Level:      int32(info.Level),
		NumRecords: info.NumEntries,
		Size:       info.Size,
		IndexFile:  info.IndexFile,
		DataFile:   info.DataFile,
		BloomFile:  info.BloomFile,
	}
}
