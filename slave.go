package main

import "github.com/gorilla/mux"

var slaveCommand = &command{
	name: "slave",
}

func init() {
	slaveCommand.run = runSlave
}

var (
	sport = slaveCommand.flag.Int("port", 9333, "set slave's port")
	sip   = slaveCommand.flag.String("ip", "localhost", "set slave's ip address")
)

func runSlave() {
	r := mux.NewRouter()
	r.HandleFunc("/upload", nil) //???
}
