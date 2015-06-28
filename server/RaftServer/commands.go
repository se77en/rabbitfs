package RaftServer

import (
	"fmt"

	"github.com/goraft/raft"
)

func init() {
	raft.RegisterCommand(&CreateVolCommand{})
}

type CreateVolCommand struct {
	Key []byte
	Val []byte
}

func NewCreateVolCommand(key, val []byte) *CreateVolCommand {
	return &CreateVolCommand{
		Key: key,
		Val: val,
	}
}

// CommandName implements goraft Command interface's CommandName function
// It returns a string "put"
func (pcmd *CreateVolCommand) CommandName() string {
	return "put"
}

// Apply implements goraft Command interface's Apply function
// It puts a key-value pair in KVstore
func (pcmd *CreateVolCommand) Apply(server raft.Server) (interface{}, error) {
	fmt.Println(server.Name(), "apply put: key: ", string(pcmd.Key))
	// if server.Name() == server.Leader() {
	// 	return nil, nil
	// }
	// kvs := server.Context().(KVstore)
	// err := kvs.Put(pcmd.Key, pcmd.Val)
	return nil, nil
}
