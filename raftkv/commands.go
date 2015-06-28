package raftkv

import (
	"fmt"

	"github.com/goraft/raft"
)

func init() {
	raft.RegisterCommand(&putCommand{})
	raft.RegisterCommand(&delCommand{})
	raft.RegisterCommand(&getCommand{})
}

type putCommand struct {
	Key []byte
	Val []byte
}

func newPutCommand(key, val []byte) *putCommand {
	return &putCommand{
		Key: key,
		Val: val,
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
	fmt.Println("apply put: ", server.Name())
	if server.Name() == server.Leader() {
		return nil, nil
	}
	kvs := server.Context().(KVstore)
	err := kvs.Put(pcmd.Key, pcmd.Val)
	return nil, err
}

type delCommand struct {
	Key []byte
}

func newDelCommand(key []byte) *delCommand {
	return &delCommand{
		Key: key,
	}
}

func (dcmd *delCommand) CommandName() string {
	return "delete"
}

func (dcmd *delCommand) Apply(server raft.Server) (interface{}, error) {
	if server.Name() == server.Leader() {
		return nil, nil
	}
	kvs := server.Context().(KVstore)
	err := kvs.Delete(dcmd.Key)
	return nil, err
}

type getCommand struct {
	Key []byte
}

func newGetCommand(key []byte) *getCommand {
	return &getCommand{
		Key: key,
	}
}

func (gcmd *getCommand) CommandName() string {
	return "get"
}

func (gcmd *getCommand) Apply(server raft.Server) (interface{}, error) {
	return nil, nil
}
