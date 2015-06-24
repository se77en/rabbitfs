package storage

import "github.com/syndtr/goleveldb/leveldb"

type Mapping struct {
	db *leveldb.DB
}

func NewLevelDBMapping(filename string) (*Mapping, error) {
	// kvs, err := raftkv.NewLevelDB(filename)
	ldb, err := leveldb.OpenFile(filename, nil)
	if err != nil {
		return nil, err
	}
	return &Mapping{db: ldb}, nil
}

func (m *Mapping) Put(key uint64, cookie uint32, offset uint32, size uint32) error {
	keyBytes := make([]byte, 12)
	UInt64ToBytes(keyBytes[0:8], key)
	UInt32ToBytes(keyBytes[8:12], cookie)
	val := make([]byte, 8)
	UInt32ToBytes(val[0:4], offset)
	UInt32ToBytes(val[4:8], size)
	return m.db.Put(keyBytes, val, nil)
}

func (m *Mapping) Get(key uint64, cookie uint32) (offset uint32, size uint32, err error) {
	keyBytes := make([]byte, 12)
	UInt64ToBytes(keyBytes[0:8], key)
	UInt32ToBytes(keyBytes[8:12], cookie)
	val, err := m.db.Get(keyBytes, nil)
	if err != nil {
		return 0, 0, err
	}
	return BytesToUInt32(val[0:4]), BytesToUInt32(val[4:8]), err
}

func (m *Mapping) Del(key uint64, cookie uint32) error {
	keyBytes := make([]byte, 12)
	UInt64ToBytes(keyBytes[0:8], key)
	UInt32ToBytes(keyBytes[8:12], cookie)
	return m.db.Delete(keyBytes, nil)
}

func (m *Mapping) Iter(mapIterFunc func(key uint64, cookie uint32) error) error {
	iter := m.db.NewIterator(nil, nil)
	for iter.Next() {
		keyBytes := iter.Key()
		key := BytesToUInt64(keyBytes[0:8])
		cookie := BytesToUInt32(keyBytes[8:12])
		if err := mapIterFunc(key, cookie); err != nil {
			return err
		}
	}
	iter.Release()
	return iter.Error()
}
