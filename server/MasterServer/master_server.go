package MasterServer

import (
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/raftkv"
)

const (
	keyCurrentNeedleID = "current.needle.id"
	keyCurrentVolID    = "current.volume.id"
)

type MasterServer struct {
	raftkv              *raftkv.Raftkv
	router              *mux.Router
	logicVolumes        []logicVolume
	logicVolumeConfFile *os.File
}

// NewMasterServer returns a new MasterServer
func NewMasterServer(
	logicVolumeConfPath string,
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
	if err != nil {
		return nil, err
	}
	ms.raftkv, err = raftkv.NewRaftkv(
		peers,
		kvs,
		raftkvPath,
		Addr,
		port,
		raftkvTransporterPrefix,
		raftkvTransporterTimeout,
		raftkvPulse)
	if err != nil {
		return nil, err
	}
	// run the raftkv
	go ms.raftkv.ListenAndServe()
	ms.logicVolumeConfFile, err = os.OpenFile(logicVolumeConfPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()
	r.HandleFunc("/dir/assign", ms.redirectToLeader(ms.assignFileIDHandler))
	r.HandleFunc("/vol/assign", ms.redirectToLeader(ms.assignVolumeHandler))
	return
}

func (ms *MasterServer) redirectToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.raftkv.Leader() == ms.raftkv.Name() {
			f(w, r)
		} else {
			http.Redirect(w, r, ms.raftkv.Leader()+"/vol/assign", http.StatusFound)
		}
	}
}
