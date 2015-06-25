package raftkv

// KVstore interface has get and set function to be implemented
type KVstore interface {
	Get(key []byte) ([]byte, error)
	Put(key, val []byte) error
	Delete(key []byte) error
	Iter(func(keyBytes []byte) error) error
}
