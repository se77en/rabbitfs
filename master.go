package main

import (
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/log"
)

var masterCommand = &command{
	name: "master",
}

func init() {
	masterCommand.run = runMaster
}

var (
	mport   = masterCommand.flag.Int("port", 9333, "set master's port")
	mIP     = masterCommand.flag.String("ip", "localhost", "set master's ip address")
	mPeers  = masterCommand.flag.String("peers", "", "ip:port, ip:port, ...")
	mFolder = masterCommand.flag.String("mdir", os.TempDir(), "data directory to store meta data")
)

type master struct {
	// TODO: fill this master struct
	rs raftServer
}

func runMaster() {
	r := mux.NewRouter()
	r.HandleFunc("{fileID}", getFileHandler).Methods("GET")
	r.HandleFunc("/upload", uploadHandler).Methods("POST")
	r.HandleFunc("/update", updateHandler).Methods("POST")
	r.HandleFunc("/assign", assignHandler)

	// Listening address
	la := *mIP + ":" + strconv.Itoa(*mport)
	log.Infoln(0, "master listening on: ", la)

	// master peers
	peers := strings.Split(*mPeers, ",")
	// transporter := raft.NewHTTPTransporter("/raft", 0)
	mst := &master{}
	raft.NewServer(la, *mFolder, transporter, nil, mst, "")

	err := http.ListenAndServe(la, r)
	if err != nil {
		log.Fatalf("ListenAndServe error: %s", err.Error())
	}
}

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "reading file")
	// TODO: add reading file process
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "uploading file")
	// TODO: add uploading file process

}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "updating file")
	// TODO: add updating file process
}

func assignHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "assigning file")
	// TODO: add assign file process
}
