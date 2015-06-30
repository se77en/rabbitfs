package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/lilwulin/rabbitfs/helper"
	"github.com/twinj/uuid"
)

type createVolResult struct {
	VolumeIDIP
	Error string `json:"error,omitempty"`
}

type assignFileIDResult struct {
	FID   string `json:"fileid,omitempty"`
	VolIP string `json:"volume_ip,omitempty"`
	Error string `json:"error,omitempty"`
}

func (dir *Directory) assignFileIDHandler(w http.ResponseWriter, r *http.Request) {
	u4 := uuid.NewV4()
	keyBytes := u4.Bytes()
	needleid := helper.BytesToUInt64(keyBytes[:8])
	volIDIP, err := dir.pickVolume(r.FormValue("replication"))
	if err != nil {
		helper.WriteJson(w, assignFileIDResult{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	cookie := rand.Uint32()
	fid := fmt.Sprintf("%d,%d,%d", volIDIP.ID, needleid, cookie)
	a := assignFileIDResult{
		FID:   fid,
		VolIP: volIDIP.IP[rand.Intn(len(volIDIP.IP))],
	}
	helper.WriteJson(w, a, http.StatusOK)
}

func (dir *Directory) createVolumeHandler(w http.ResponseWriter, r *http.Request) {
	// increase volumeID
	createVolCmd := &CreateVolCommand{ReplicateStr: r.FormValue("replication")}
	// fmt.Println("replication: ", createVolCmd.ReplicateStr)
	v, err := dir.raftServer.Do(createVolCmd)
	if err != nil {
		helper.WriteJson(w, createVolResult{Error: err.Error()}, http.StatusInternalServerError)
		return
	}
	volidip := v.(VolumeIDIP)
	for _, ip := range volidip.IP {
		var b bytes.Buffer
		bytes, err := json.Marshal(volidip)
		if err != nil {
			helper.WriteJson(w, createVolResult{Error: err.Error()}, http.StatusInternalServerError)
			return
		}
		b.Write(bytes)
		_, err = postAndError(fmt.Sprintf("http://%s/vol/create", ip), "application/json", &b)
		if err != nil {
			helper.WriteJson(w, createVolResult{Error: err.Error()}, http.StatusInternalServerError)
			return
		}
	}
	helper.WriteJson(w, volidip, http.StatusOK)
}

func (dir *Directory) updateVolumeInfoHandler(w http.ResponseWriter, r *http.Request) {
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	volInfo := volumeInfo{}
	if err = json.Unmarshal(bytes, &volInfo); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	dir.volInfoMap[volInfo.ID] = volInfo
}
