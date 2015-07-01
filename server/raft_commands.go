package server

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/chrislusf/raft"
)

func init() {
	raft.RegisterCommand(&CreateVolCommand{})
}

type CreateVolCommand struct {
	ReplicateStr string
}

// CommandName implements goraft Command interface's CommandName function
// It returns a string "put"
func (c *CreateVolCommand) CommandName() string {
	return "create.volume"
}

// Apply implements goraft Command interface's Apply function
// It puts a key-value pair in KVstore
func (c *CreateVolCommand) Apply(server raft.Server) (interface{}, error) {
	dir := server.Context().(*Directory)
	maxVolID := uint32(0)
	for _, volidip := range dir.volIDIPs {
		if volidip.ID > maxVolID {
			maxVolID = volidip.ID
		}
	}
	maxVolID++
	storeIPs, err := dir.pickStoreServer(c.ReplicateStr)
	if err != nil {
		return nil, err
	}
	volIDIP := VolumeIDIP{
		ID: maxVolID,
		IP: storeIPs,
	}
	dir.volIDIPs = append(dir.volIDIPs, volIDIP)
	bytes, err := json.Marshal(dir.volIDIPs)
	if err = ioutil.WriteFile(filepath.Join(dir.confPath, "vol.conf.json"), bytes, 0644); err != nil {
		return nil, err
	}
	return volIDIP, nil
}
