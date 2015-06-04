package storage

import "github.com/lilwulin/rabbitfs/raftkv"

type Mapping struct {
	kvstore raftkv.KVstore
}

func NewLevelDBMapping(filename string) (*Mapping, error) {
	kvs, err := raftkv.NewLevelDB(filename)
	if err != nil {
		return nil, err
	}
	return &Mapping{kvstore: kvs}, nil
}

func (m *Mapping) Put(key uint64, cookie uint32, offset uint32, size uint32) error {
	// TODO: fill this
	keyBytes := make([]byte, 12)
	UInt64ToBytes(keyBytes[0:8], key)
	UInt32ToBytes(keyBytes[8:12], cookie)
	val := make([]byte, 8)
	UInt32ToBytes(val[0:4], offset)
	UInt32ToBytes(val[4:8], size)
	return m.kvstore.Put(keyBytes, val)
}

func (m *Mapping) Get(key uint64, cookie uint32) (offset uint32, size uint32, err error) {
	keyBytes := make([]byte, 12)
	UInt64ToBytes(keyBytes[0:8], key)
	UInt32ToBytes(keyBytes[8:12], cookie)
	val, err := m.kvstore.Get(keyBytes)
	return BytesToUInt32(val[0:4]), BytesToUInt32(val[4:8]), err
}

func (m *Mapping) Del(key uint64, cookie uint32) error {
	keyBytes := make([]byte, 12)
	UInt64ToBytes(keyBytes[0:8], key)
	UInt32ToBytes(keyBytes[8:12], cookie)
	return m.kvstore.Delete(keyBytes)
}
