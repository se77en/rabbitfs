package raftkv

import (
	"time"

	"github.com/goraft/raft"
)

// Raftkv contains raft server and a KVstore
type Raftkv struct {
	peers  []string
	server raft.Server
	kvs    KVstore
}

// NewRaftkv returns a new Raftkv and an error
func NewRaftkv(
	peers []string,
	kvs KVstore,
	dir string,
	connectionString string,
	transporterPrefix string,
	transporterTimeout time.Duration,
) (rs *Raftkv, err error) {
	rs = &Raftkv{
		peers: peers,
		kvs:   kvs,
	}
	transporter := raft.NewHTTPTransporter(transporterPrefix, transporterTimeout)
	rs.server, err = raft.NewServer(connectionString, dir, transporter, nil, rs.kvs, connectionString)

	// TODO: need to add peers

	return rs, nil
}

// Get gets a value by key
func (rkv *Raftkv) Get(key []byte) ([]byte, error) {
	return rkv.kvs.Get(key)
}

// Put puts a key-value pair, it overwrites the old one.
func (rkv *Raftkv) Put(key, val []byte) error {
	_, err := rkv.server.Do(newPutCommand(key, val))
	return err
}

// Del deletes a key-value pair
func (rkv *Raftkv) Del(key []byte) error {
	_, err := rkv.server.Do(newDelCommand(key))
	return err
}
