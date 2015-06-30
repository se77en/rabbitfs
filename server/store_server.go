package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/storage"
)

type StoreServer struct {
	router           *mux.Router
	volumeMap        map[uint32]*storage.Volume
	garbageThreshold float32
	volumeDir        string
	Addr             string
	timeout          time.Duration
	localVolIDIPs    []VolumeIDIP
	conf             configuration
}

func NewStoreServer(
	confPath string,
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

	// read configuration file
	confFile, err := os.OpenFile(filepath.Join(confPath, "rabbitfs.conf.json"), os.O_RDWR|os.O_CREATE, 0644)
	defer confFile.Close()
	if err != nil {
		return nil, err
	}
	// confBytes, err := ioutil.ReadFile(filepath.Join(confPath, "rabbitfs.conf.json"))
	confBytes, err := ioutil.ReadAll(confFile)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(confBytes, &ss.conf); err != nil {
		return nil, err
	}

	if err = ss.loadVolumes(volumeDir); err != nil {
		return nil, err
	}
	volIDIPFile, err := os.OpenFile(filepath.Join(volumeDir, "volIDIPs.json"), os.O_RDWR|os.O_CREATE, 0644)
	defer volIDIPFile.Close()
	if err != nil {
		return nil, err
	}
	volidipBytes, err := ioutil.ReadAll(volIDIPFile)
	if err != nil {
		return nil, err
	}
	if len(volidipBytes) > 0 {
		if err = json.Unmarshal(volidipBytes, &ss.localVolIDIPs); err != nil {
			return nil, err
		}
	}

	// ss.keepSendingHearbeats()

	ss.router.HandleFunc("/{fileID}", ss.uploadHandler).Methods("POST")
	ss.router.HandleFunc("/{fileID}", ss.getFileHandler).Methods("GET")
	ss.router.HandleFunc("/replicate/{fileID}", ss.replicateUploadHandler).Methods("POST")
	ss.router.HandleFunc("/vol/create", ss.createVolumeHandler).Methods("POST")
	ss.router.HandleFunc("/store/stat", ss.getStatHandler)
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
		panic("store server error: " + err.Error())
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

// func (ss *StoreServer) keepSendingHearbeats() {
// 	ticker := time.NewTicker(ss.pulse)
// 	for range ticker.C {

// 	}
// }
