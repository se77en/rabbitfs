package main

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/log"
	"github.com/lilwulin/rabbitfs/raftkv"
)

var masterCommand = &command{
	name: "master",
}

func init() {
	masterCommand.run = runMaster
}

var (
	mPort      = masterCommand.flag.Int("port", 9333, "set master's port")
	mIP        = masterCommand.flag.String("ip", "localhost", "set master's ip address")
	mPeers     = masterCommand.flag.String("peers", "", "ip:port,ip:port,...")
	mFolder    = masterCommand.flag.String("mdir", os.TempDir(), "data directory to store meta data")
	mKVtimeout = masterCommand.flag.Duration("kvtimeout", 500, "set raftkv's timeout(ms)")
)

type master struct {
	// TODO: fill this master struct

}

func runMaster() {
	r := mux.NewRouter()
	r.HandleFunc("{fileID}", getFileHandler).Methods("GET")
	r.HandleFunc("/update", updateHandler)
	r.HandleFunc("/assign", assignHandler)
	r.HandleFunc("/vol/grow", growHandler)

	// Listening address
	la := *mIP + ":" + strconv.Itoa(*mPort)
	log.Infoln(0, "master listening on: ", la)

	// master peers
	peers := strings.Split(*mPeers, ",")
	go func() {
		time.Sleep(500 * time.Millisecond)
		kvs, err := raftkv.NewLevelDB(*mFolder + "/leveldb/" + la)
		if err != nil {
			log.Fatalln(err)
		}
		rkv1, err := raftkv.NewRaftkv(
			peers, kvs,
			*mFolder+"/raft"+la+"/", *mIP,
			*mPort, "/raftkv",
			*mKVtimeout*time.Millisecond, 0)
		if err != nil {
			log.Fatalln(err)
		}
		rkv1.ListenAndServe()
	}()
	// // transporter := raft.NewHTTPTransporter("/raft", 0)
	// mst := &master{}
	// raft.NewServer(la, *mFolder, transporter, nil, mst, "")

	err := http.ListenAndServe(la, r)
	if err != nil {
		log.Fatalf("ListenAndServe error: %s", err.Error())
	}
}
