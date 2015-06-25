package StoreServer

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/storage"
)

type StoreServer struct {
	router    *mux.Router
	volumeMap map[uint32]*storage.Volume
}

func NewStoreServer(volumesPathMap map[uint32]string, garbageThreshold float32) (ss *StoreServer, err error) {
	ss = &StoreServer{}
	for id, path := range volumesPathMap {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		v, err := storage.NewVolume(id, file, filepath.Dir(path)+fmt.Sprintf("vol%d_needle_map", id), garbageThreshold)
		if err != nil {
			return nil, err
		}
		ss.volumeMap[id] = v
	}
	r := mux.NewRouter()
	r.HandleFunc("/{fileID}", ss.uploadHandler).Methods("POST")
	r.HandleFunc("/{fileID}", ss.getFileHandler).Methods("GET")
	r.HandleFunc("/vol/{volID}", ss.createVolume).Methods("POST")
	return
}
