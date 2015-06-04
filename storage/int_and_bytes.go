package storage

import "encoding/binary"

func UInt32ToBytes(b []byte, i uint32) {
	binary.LittleEndian.PutUint32(b, i)
}

func BytesToUInt32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func UInt64ToBytes(b []byte, i uint64) {
	binary.LittleEndian.PutUint64(b, i)
}

func BytesToUInt64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
