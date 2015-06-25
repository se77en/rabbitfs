package master_server

import (
	"time"

	"github.com/lilwulin/rabbitfs/raftkv"
)

type MasterServer struct {
	raftkv *raftkv.Raftkv
}

func NewMasterServer(
	peers []string,
	Addr string,
	port int,
	raftkvPath string,
	keyValueStorePath string,
	raftkvTransporterPrefix string,
	raftkvTransporterTimeout time.Duration,
	raftkvPulse time.Duration,
) (ms *MasterServer, err error) {
	ms = &MasterServer{}
	kvs, err := raftkv.NewLevelDB(keyValueStorePath)
	ms.raftkv, err = raftkv.NewRaftkv(
		peers,
		kvs,
		raftkvPath,
		Addr,
		port,
		raftkvTransporterPrefix,
		raftkvTransporterTimeout,
		raftkvPulse)
	go ms.raftkv.ListenAndServe()
	return
}
