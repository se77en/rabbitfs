package main

import (
	"github.com/goraft/raft"
	"github.com/lilwulin/rabbitfs/log"
)

type raftServer struct {
	peers  []string
	server raft.Server
}

func newRaftServer(peers []string, mst master, masterAddress string, dir string, context interface{}, mpulse int) *raftServer {
	log.Infoln(0, "generating new raft server")
	rs := &raftServer{
		peers: peers,
	}
	transporter := raft.NewHTTPTransporter("/raft", 0)
	var err error
	rs.server, err = raft.NewServer(masterAddress, dir, transporter, nil, mst, "")
	if err != nil {
		log.Fatalf("create raft server failed: %s", err.Error())
	}

	// transporter.Install(rs.server, rs)
	return rs
}
