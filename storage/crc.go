package storage

import "hash/crc32"

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func newCheckSum(b []byte) uint32 {
	return crc32.Checksum(b, castagnoliTable)
}
