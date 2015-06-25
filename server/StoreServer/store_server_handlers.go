package StoreServer

import (
	"net/http"

	"github.com/gorilla/mux"
)

func (ss *StoreServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileIDStr := vars["fileID"]
	// TODO: fill this
}

func (ss *StoreServer) getFileHandler(w http.ResponseWriter, r *http.Request) {

}

func (ss *StoreServer) createVolume(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	volIDStr := vars["volID"]
	// TODO: fill this
}
