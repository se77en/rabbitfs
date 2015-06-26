package StoreServer

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/storage"
)

func (ss *StoreServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileIDStr := vars["fileID"]
	volID, needleID, cookie, err := newFileID(fileIDStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (ss *StoreServer) getFileHandler(w http.ResponseWriter, r *http.Request) {

}

func (ss *StoreServer) createVolumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	volIDStr := vars["volID"]
	volID, err := newVolumeID(volIDStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	volPath := filepath.Join(ss.volumeDir, volIDStr+".vol")
	needleMapPath := filepath.Join(ss.volumeDir, fmt.Sprintf("needle_map_vol%d", volID))
	file, err := os.OpenFile(volPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	v, err := storage.NewVolume(volID, file, needleMapPath, ss.garbageThreshold)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ss.volumeMap[volID] = v
}

func newVolumeID(volIDStr string) (uint32, error) {
	id, err := strconv.ParseUint(volIDStr, 10, 64)
	return uint32(id), err
}

func newFileID(fileIDStr string) (uint32, uint64, uint32, error) {
	strs := strings.Split(fileIDStr, ",")
	if len(strs) < 3 {
		return 0, 0, 0, fmt.Errorf("illegal fileID formated: %s", fileIDStr)
	}
	volIDStr, needleIDStr, cookieStr := strs[0], strs[1], strs[2]
	volID, err := newVolumeID(volIDStr)
	if err != nil {
		return 0, 0, 0, err
	}
	needleID, err := strconv.ParseUint(needleIDStr, 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	cookie, err := strconv.ParseUint(cookieStr, 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	return volID, needleID, uint32(cookie), nil
}
