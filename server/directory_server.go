package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"code.google.com/p/log4go"

	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/helper"
)

const (
	keyCurrentNeedleID = "current.needle.id"
	keyCurrentVolID    = "current.volume.id"
)

var (
	transport = &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{Transport: transport}
)

// Directory is the object that handle the fileid assigning and volume creating
type Directory struct {
	router        *mux.Router
	volumeMaxSize int64
	volIDIPs      []VolumeIDIP
	volInfoMap    map[uint32]volumeInfo
	confPath      string
	conf          configuration
	Addr          string
	timeout       time.Duration
	raftServer    *RaftServer
	pulse         time.Duration
	storeStatMap  map[string]storeStat
}

type storeStat struct {
	IsAlive   bool         `json:"is_alive"`
	VolsCount uint32       `json:"vols_count,omitempty"`
	VolsInfo  []volumeInfo `json:"vols_info,omitempty"`
	ErrStr    string       `json:"error,omitempty"`
}

type configuration struct {
	Directories []string `json:"directory,omitempty"`
	Stores      []string `json:"store,omitempty"`
}

// NewDirectory returns a new Directory
func NewDirectory(
	confPath string,
	Addr string,
	raftPath string,
	pulse time.Duration,
	volumeMaxSize int64,
	serverTimeout time.Duration,
	raftTransporterTimeout time.Duration,
	raftPulse time.Duration,
) (dir *Directory, err error) {
	dir = &Directory{
		confPath:      confPath,
		router:        mux.NewRouter(),
		volIDIPs:      make([]VolumeIDIP, 0),
		Addr:          Addr,
		timeout:       serverTimeout,
		volumeMaxSize: volumeMaxSize * 1024 * 1024,
		pulse:         pulse,
		storeStatMap:  map[string]storeStat{},
		volInfoMap:    map[uint32]volumeInfo{},
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
	if err = json.Unmarshal(confBytes, &dir.conf); err != nil {
		return nil, err
	}

	// Run raft server
	dirAddrs := make([]string, len(dir.conf.Directories))
	copy(dirAddrs, dir.conf.Directories)
	dir.raftServer, err = NewRaftServer(
		dir, dirAddrs, filepath.Join(confPath, "raft"), dir.Addr,
		dir.router, "/raft", raftTransporterTimeout, raftPulse,
	)
	if err != nil {
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
	dir.router.HandleFunc("/dir/assign", dir.proxyToLeader(dir.assignFileIDHandler))
	dir.router.HandleFunc("/vol/create", dir.proxyToLeader(dir.createVolumeHandler))
	dir.router.HandleFunc("/vol/info", dir.proxyToLeader(dir.updateVolumeInfoHandler))
	// dir.router.HandleFunc("/store/hearbeat", dir.proxyToLeader(dir.heartbeatHandler))
	go dir.tickerGetStoreStat()
	return
}

func (dir *Directory) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if dir.raftServer.Leader() == dir.raftServer.Name() {
			f(w, r)
		} else {
			fmt.Println("not leader replication: ", r.FormValue("replication"))
			targetURL, err := url.Parse(dir.raftServer.Leader())
			if err != nil {
				helper.WriteJson(w, err.Error(), http.StatusInternalServerError)
				return
			}
			log4go.Info("proxying to raft leader: %s", dir.raftServer.Leader())
			proxy := httputil.NewSingleHostReverseProxy(targetURL)
			proxy.Transport = transport
			proxy.ServeHTTP(w, r)
		}
	}
}

func (dir *Directory) ListenAndServe() {
	connectString := dir.Addr
	if connectString[:7] == "http://" {
		connectString = connectString[7:]
	}
	s := &http.Server{
		Addr:         dir.Addr,
		Handler:      dir.router,
		ReadTimeout:  dir.timeout,
		WriteTimeout: dir.timeout,
	}
	if err := s.ListenAndServe(); err != nil {
		panic("directory server failed: " + err.Error())
	}
}

func (dir *Directory) pickVolume(replicateStr string) (*VolumeIDIP, error) {
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
	candidateVolIDIP := []VolumeIDIP{}
	for _, volIDIP := range dir.volIDIPs {
		if len(volIDIP.IP) == replicateCount &&
			dir.volInfoMap[volIDIP.ID].Size < dir.volumeMaxSize {
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
	if replicateCount < 1 {
		return nil, fmt.Errorf("replicate count must be greater than 0")
	}
	if replicateCount > len(dir.conf.Stores) {
		return nil, fmt.Errorf("does't have enough store machine for replication")
	}
	storesTmp := []string{}
	candidateStore := []string{}
	for _, store := range dir.conf.Stores {
		if dir.storeStatMap[store].IsAlive == true {
			storesTmp = append(storesTmp, store)
		}
	}
	if len(storesTmp) == 0 {
		return nil, fmt.Errorf("does't have enough store machine")
	}
	// stores = append(candidateStore, dir.conf.Stores...)
	for i := 0; i < replicateCount; i++ {
		pickedIndex := rand.Intn(len(storesTmp))
		candidateStore = append(candidateStore, storesTmp[pickedIndex])
		storesTmp = append(storesTmp[:pickedIndex], storesTmp[pickedIndex+1:]...)
	}
	return candidateStore, nil
}

func (dir *Directory) tickerGetStoreStat() {
	for _, storeAddr := range dir.conf.Stores {
		go func(storeAddr string) {
			ticker := time.NewTicker(dir.pulse)
			for range ticker.C {
				resp, err := client.Get("http://" + storeAddr + "/store/stat")
				if err != nil {
					log4go.Error(err.Error())
					continue
				}
				if resp.StatusCode != http.StatusOK {
					log4go.Error("connect to " + storeAddr + " failed")
					dir.storeStatMap[storeAddr] = storeStat{
						IsAlive: false,
					}
				} else {
					bytes, _ := ioutil.ReadAll(resp.Body)
					stat := storeStat{}
					json.Unmarshal(bytes, &stat)
					dir.storeStatMap[storeAddr] = stat
					for _, volInfo := range stat.VolsInfo {
						dir.volInfoMap[volInfo.ID] = volInfo
					}
				}
			}
		}(storeAddr)
	}
}
