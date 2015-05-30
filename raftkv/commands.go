package raftkv

import "github.com/goraft/raft"

type putCommand struct {
	key []byte
	val []byte
}

func newPutCommand(key, val []byte) *putCommand {
	return &putCommand{
		key: key,
		val: val,
	}
}

// CommandName implements goraft Command interface's CommandName function
// It returns a string "put"
func (pcmd *putCommand) CommandName() string {
	return "put"
}

// Apply implements goraft Command interface's Apply function
// It puts a key-value pair in KVstore
func (pcmd *putCommand) Apply(server raft.Server) (interface{}, error) {
	kvs := server.Context().(KVstore)
	err := kvs.Put(pcmd.key, pcmd.val)
	return nil, err
}

type delCommand struct {
	key []byte
}

func newDelCommand(key []byte) *delCommand {
	return &delCommand{
		key: key,
	}
}

func (dcmd *delCommand) CommandName() string {
	return "delete"
}

func (dcmd *delCommand) Apply(server raft.Server) (interface{}, error) {
	kvs := server.Context().(KVstore)
	err := kvs.Delete(dcmd.key)
	return nil, err
}
