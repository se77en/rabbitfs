package StoreServer

import (
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/helper"
	"github.com/lilwulin/rabbitfs/log"
	"github.com/lilwulin/rabbitfs/storage"
)

type result struct {
	Name  string `json:"name,omitempty"`
	Size  int    `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
}

func (ss *StoreServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileIDStr := vars["fileID"]
	volID, needleID, cookie, err := newFileID(fileIDStr)
	if err != nil {
		helper.WriteJson(w, result{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	data, name, err := parseUpload(r)
	if err != nil {
		helper.WriteJson(w, result{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	n := storage.NewNeedle(cookie, needleID, data, name)
	if err = ss.volumeMap[volID].AppendNeedle(n); err != nil {
		helper.WriteJson(w, result{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	res := result{
		Name: string(name),
		Size: len(data),
	}
	helper.WriteJson(w, res, http.StatusOK)
}

func (ss *StoreServer) getFileHandler(w http.ResponseWriter, r *http.Request) {
	fileIDStr := mux.Vars(r)["fileID"]
	if li := strings.LastIndex(fileIDStr, "."); li != -1 {
		fileIDStr = fileIDStr[:li]
	}
	volID, needleID, cookie, err := newFileID(fileIDStr)
	if err != nil {
		helper.WriteJson(w, result{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	n, err := ss.volumeMap[volID].GetNeedle(needleID, cookie)
	if err != nil {
		helper.WriteJson(w, result{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	filename := string(n.Name)
	dotIndex := strings.LastIndex(filename, ".")
	contentType := ""
	if dotIndex > 0 {
		ext := filename[dotIndex:]
		contentType = mime.TypeByExtension(ext)
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("filename=\"%s\"", filename))
	w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
	_, err = w.Write(n.Data)
	if err != nil {
		log.Errorln(err)
	}
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

func parseUpload(r *http.Request) ([]byte, []byte, error) {
	form, err := r.MultipartReader()
	if err != nil {
		return nil, nil, err
	}
	filename := ""
	var data []byte
	for filename == "" {
		part, err := form.NextPart()
		if err != nil {
			return nil, nil, err
		}
		filename = part.FileName()
		if data, err = ioutil.ReadAll(part); err != nil {
			return nil, nil, err
		}
	}
	filename = filepath.Base(filename)
	return data, []byte(filename), err
}
