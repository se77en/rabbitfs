package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"code.google.com/p/log4go"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/helper"
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
	if ss.volumeMap[volID] == nil {
		helper.WriteJson(w, result{Error: fmt.Sprintf("no volume %d", volID)}, http.StatusInternalServerError)
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

	fi, _ := ss.volumeMap[volID].StoreFile.Stat()
	vi := volumeInfo{
		ID:   volID,
		Size: fi.Size(),
	}
	viBytes, _ := json.Marshal(vi)
	for i := range ss.conf.Directories { // send volume information to directory server
		var b bytes.Buffer
		b.Write(viBytes)
		_, err := postAndError("http://"+ss.conf.Directories[i]+"/vol/info", "application/json", &b)
		if err == nil {
			break
		} else {
			log4go.Warn("send volumeInfo to directory get err: %s", err.Error())
		}
	}
	for _, localVolIDIP := range ss.localVolIDIPs {
		if localVolIDIP.ID == volID {
			for _, ip := range localVolIDIP.IP {
				if ip != ss.Addr {
					if err = replicateUpload(fmt.Sprintf("http://%s/replicate/%s", ip, fileIDStr), string(name), data); err != nil {
						helper.WriteJson(w, result{Error: err.Error()}, http.StatusInternalServerError)
						return
					}
				}
			}
			break
		}
	}
	res := result{
		Name: string(name),
		Size: len(data),
	}
	helper.WriteJson(w, res, http.StatusOK)

}

func replicateUpload(url string, filename string, data []byte) error {
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	fmt.Println(filename)
	f, _ := mw.CreateFormFile("replicate", filename)
	written, err := f.Write(data)
	if err != nil {
		return err
	}
	fmt.Println("written: ", written)
	fmt.Println(len(b.Bytes()))
	mw.Close()
	_, err = postAndError(url, mw.FormDataContentType(), &b)
	return err
}

func (ss *StoreServer) replicateUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileIDStr := vars["fileID"]
	volID, needleID, cookie, err := newFileID(fileIDStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if ss.volumeMap[volID] == nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, name, err := parseUpload(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	n := storage.NewNeedle(cookie, needleID, data, name)
	if err = ss.volumeMap[volID].AppendNeedle(n); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
	if ss.volumeMap[volID] == nil {
		helper.WriteJson(w, result{Error: fmt.Sprintf("no volume %d", volID)}, http.StatusInternalServerError)
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
	// TODO: Add ETAG
	w.Header().Set("Content-Disposition", fmt.Sprintf("filename=\"%s\"", filename))
	w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
	_, err = w.Write(n.Data)
	if err != nil {
		log4go.Error(err.Error())
	}
}

func (ss *StoreServer) createVolumeHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var volIDIP VolumeIDIP
	if err = json.Unmarshal(body, &volIDIP); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	id := volIDIP.ID
	volIDStr := fmt.Sprintf("%d", id)
	volPath := filepath.Join(ss.volumeDir, volIDStr+".vol")
	needleMapPath := filepath.Join(ss.volumeDir, fmt.Sprintf("needle_map_vol%d", id))
	file, err := os.OpenFile(volPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	v, err := storage.NewVolume(id, file, needleMapPath, ss.garbageThreshold)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ss.localVolIDIPs = append(ss.localVolIDIPs, volIDIP)
	bytes, err := json.Marshal(ss.localVolIDIPs)
	if err = ioutil.WriteFile(filepath.Join(ss.volumeDir, "volIDIPs.json"), bytes, 0644); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	ss.volumeMap[id] = v
}

func (ss *StoreServer) getStatHandler(w http.ResponseWriter, r *http.Request) {
	volsInfo := []volumeInfo{}
	for volID, vol := range ss.volumeMap {
		fi, _ := vol.StoreFile.Stat()
		fileSize := fi.Size()
		volsInfo = append(volsInfo, volumeInfo{ID: volID, Size: fileSize})
	}
	stat := storeStat{
		IsAlive:   true,
		VolsCount: uint32(len(ss.localVolIDIPs)),
		VolsInfo:  volsInfo,
	}
	bytes, _ := json.Marshal(stat)
	w.Write(bytes)
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
