package DirectoryServer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

const (
	keyCurrentNeedleID = "current.needle.id"
	keyCurrentVolID    = "current.volume.id"
)

// Directory is the object that handle the fileid assigning and volume creating
type Directory struct {
	router   *mux.Router
	volIDIPs []volumeIDIP
	confPath string
	conf     configuration
	Addr     string
	timeout  time.Duration
}

type configuration struct {
	Masters []string `json:"master,omitempty"`
	Stores  []string `json:"store,omitempty"`
}

// NewDirectory returns a new Directory
func NewDirectory(
	confPath string,
	Addr string,
	raftkvPath string,
	keyValueStorePath string,
	serverTimeout time.Duration,
	raftkvTransporterPrefix string,
	raftkvTransporterTimeout time.Duration,
	raftkvPulse time.Duration,
) (dir *Directory, err error) {
	dir = &Directory{
		confPath: confPath,
		router:   mux.NewRouter(),
		volIDIPs: make([]volumeIDIP, 0),
		Addr:     Addr,
		timeout:  serverTimeout,
	}
	confFile, err := os.OpenFile(filepath.Join(confPath, "rabbitfs.conf.json"), os.O_RDWR|os.O_CREATE, 0644)
	defer confFile.Close()
	if err != nil {
		return nil, err
	}
	// confBytes, err := ioutil.ReadFile(filepath.Join(confPath, "rabbitfs.conf.json"))
	confBytes, err := ioutil.ReadAll(confFile)
	if err != nil {
		return nil, err
	}
	// fmt.Println(string(confBytes))
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(confBytes, &dir.conf); err != nil {
		return nil, err
	}

	// volConfBytes, err := ioutil.ReadFile(filepath.Join(confPath, "vol.conf.json"))
	volConfFile, err := os.OpenFile(filepath.Join(confPath, "vol.conf.json"), os.O_RDWR|os.O_CREATE, 0644)
	defer volConfFile.Close()
	if err != nil {
		return nil, err
	}
	volConfBytes, err := ioutil.ReadAll(volConfFile)
	if err != nil {
		return nil, err
	}
	if len(volConfBytes) > 0 {
		if err = json.Unmarshal(volConfBytes, &dir.volIDIPs); err != nil {
			return nil, err
		}
	}
	dir.router.HandleFunc("/dir/assign", dir.assignFileIDHandler)
	dir.router.HandleFunc("/vol/create", dir.createVolumeHandler)
	return
}

// func (ms *Directory) redirectToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		if ms.raftkv.Leader() == ms.raftkv.Name() {
// 			f(w, r)
// 		} else {
// 			http.Redirect(w, r, ms.raftkv.Leader()+"/vol/assign", http.StatusFound)
// 		}
// 	}
// }

func (dir *Directory) ListenAndServe() {
	s := &http.Server{
		Addr:         dir.Addr,
		Handler:      dir.router,
		ReadTimeout:  dir.timeout,
		WriteTimeout: dir.timeout,
	}
	if err := s.ListenAndServe(); err != nil {
		log.Fatalf("store server failed: %s", err.Error())
	}
}

func (dir *Directory) pickVolume(replicateStr string) (*volumeIDIP, error) {
	replicateCount := 1
	if replicateStr != "" {
		var err error
		replicateCount, err = strconv.Atoi(replicateStr)
		if err != nil {
			return nil, err
		}
	}
	if replicateCount < 1 {
		return nil, fmt.Errorf("replicate count must be > 0")
	}
	candidateVolIDIP := []volumeIDIP{}
	for _, volIDIP := range dir.volIDIPs {
		if len(volIDIP.IP) == replicateCount {
			candidateVolIDIP = append(candidateVolIDIP, volIDIP)
		}
	}
	if len(candidateVolIDIP) == 0 {
		return nil, fmt.Errorf("no volume fits the replicate count %d", replicateCount)
	}
	return &candidateVolIDIP[rand.Intn(len(candidateVolIDIP))], nil
}

func (dir *Directory) pickStoreServer(replicateStr string) ([]string, error) {
	replicateCount := 1
	if replicateStr != "" {
		var err error
		replicateCount, err = strconv.Atoi(replicateStr)
		if err != nil {
			return nil, err
		}
	}
	fmt.Println(replicateCount, " ", replicateStr)
	if replicateCount < 1 {
		return nil, fmt.Errorf("replicate count must be greater than 0")
	}
	if replicateCount > len(dir.conf.Stores) {
		return nil, fmt.Errorf("does't have enough store machine for replication")
	}
	stores := []string{}
	candidateStore := []string{}
	stores = append(candidateStore, dir.conf.Stores...)
	for i := 0; i < replicateCount; i++ {
		pickedIndex := rand.Intn(len(stores))
		candidateStore = append(candidateStore, stores[pickedIndex])
		stores = append(stores[:pickedIndex], stores[pickedIndex+1:]...)
	}
	return candidateStore, nil
}
