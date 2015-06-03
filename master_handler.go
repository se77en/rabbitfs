package main

import (
	"net/http"
	"strconv"

	"github.com/lilwulin/rabbitfs/log"
)

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "reading file")
	// TODO: add reading file process
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "updating file")
	// TODO: add updating file process
}

func assignHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln(0, "assigning file")
	count, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		log.Infoln(0, err)
		count = 1
	}
	// TODO: returning assigned id requires balancing volumes, fill this later
}

func growHandler(w http.ResponseWriter, r *http.Request) {

}
