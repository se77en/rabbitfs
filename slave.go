package main

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/log"
	"github.com/lilwulin/rabbitfs/storage"
)

var slaveCommand = &command{
	name: "slave",
}

func init() {
	slaveCommand.run = runSlave
}

type slave struct {
	volumes []storage.Volume
}

var (
	sport = slaveCommand.flag.Int("port", 9333, "set slave's port")
	sip   = slaveCommand.flag.String("ip", "localhost", "set slave's ip address")
)

func runSlave() {
	r := mux.NewRouter()
	r.HandleFunc("/upload", uploadHandler)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "uploading file")
	// TODO: add uploading file process

}
