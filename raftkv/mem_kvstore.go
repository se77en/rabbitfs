package raftkv

type MemKvStore struct {
	db map[string][]byte
}

func NewMem() *MemKvStore {
	return &MemKvStore{
		db: map[string][]byte{},
	}
}

func (m *MemKvStore) Get(key []byte) ([]byte, error) {
	return m.db[string(key)], nil
}

func (m *MemKvStore) Put(key, val []byte) error {
	m.db[string(key)] = val
	return nil
}

func (m *MemKvStore) Delete(key []byte) error {
	delete(m.db, string(key))
	return nil
}

func (m *MemKvStore) Iter(f func(keyBytes []byte) error) error {
	for k := range m.db {
		if err := f([]byte(k)); err != nil {
			return err
		}
	}
	return nil
}
