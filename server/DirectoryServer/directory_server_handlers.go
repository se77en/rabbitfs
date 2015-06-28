package DirectoryServer

import (
	"fmt"
	"math/rand"
	"net/http"

	"github.com/lilwulin/rabbitfs/helper"
	"github.com/twinj/uuid"
)

type createVolResult struct {
	volumeIDIP
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
	// TODO: allow user to specify the ip address
	// storeIPs, err := dir.pickStoreServer(r.FormValue("replication"))
	// if err != nil {
	// 	helper.WriteJson(w, createVolResult{Error: err.Error()}, http.StatusInternalServerError)
	// 	return
	// }

	// TODO: increase volID
	// helper.WriteJson(w, createVolResult{
	// 	volumeIDIP: volumeIDIP{ID: 1, IP: storeIPs},
	// }, http.StatusOK)

}
