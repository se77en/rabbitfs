package StoreServer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/log"
	"github.com/lilwulin/rabbitfs/storage"
)

type StoreServer struct {
	router           *mux.Router
	volumeMap        map[uint32]*storage.Volume
	garbageThreshold float32
	volumeDir        string
	Addr             string
	timeout          time.Duration
}

func NewStoreServer(
	volumeDir string,
	garbageThreshold float32,
	Addr string,
	timeout time.Duration,
) (ss *StoreServer, err error) {
	ss = &StoreServer{
		garbageThreshold: garbageThreshold,
		router:           mux.NewRouter(),
		volumeMap:        make(map[uint32]*storage.Volume),
		volumeDir:        volumeDir,
		Addr:             Addr,
		timeout:          timeout,
	}
	if err = ss.loadVolumes(volumeDir); err != nil {
		return nil, err
	}
	ss.router.HandleFunc("/{fileID}", ss.uploadHandler).Methods("POST")
	ss.router.HandleFunc("/{fileID}", ss.getFileHandler).Methods("GET")
	ss.router.HandleFunc("/vol/{volID}", ss.createVolumeHandler).Methods("POST")
	return
}

func (ss *StoreServer) ListenAndServe() {
	s := &http.Server{
		Addr:         ss.Addr,
		Handler:      ss.router,
		ReadTimeout:  ss.timeout,
		WriteTimeout: ss.timeout,
	}
	if err := s.ListenAndServe(); err != nil {
		log.Fatalf("store server failed: %s", err.Error())
	}
}

func (ss *StoreServer) loadVolumes(volumeDir string) error {
	dirs, err := ioutil.ReadDir(volumeDir)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		volName := dir.Name()
		if !dir.IsDir() && strings.HasSuffix(volName, ".vol") {
			volPath := filepath.Join(ss.volumeDir, volName)
			file, err := os.OpenFile(volPath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return err
			}
			idStr := volName[:len(volName)-len(".vol")]
			id, err := newVolumeID(idStr)
			if err != nil {
				return err
			}
			needleMapPath := filepath.Join(ss.volumeDir, fmt.Sprintf("needle_map_vol%d", id))
			v, err := storage.NewVolume(id, file, needleMapPath, ss.garbageThreshold)
			if err != nil {
				return err
			}
			ss.volumeMap[id] = v
		}
	}
	return nil
}
