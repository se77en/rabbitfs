package raftkv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/goraft/raft"
	"github.com/gorilla/mux"
)

var logger = log.New(os.Stdout, "[naivekv]", log.Lmicroseconds)

// Raftkv contains raft server and a KVstore
type Raftkv struct {
	peers    []string
	server   raft.Server
	kvs      KVstore
	dataDir  string
	router   *mux.Router
	httpAddr string
	port     int
}

// NewRaftkv returns a new Raftkv and an error
func NewRaftkv(
	peers []string,
	kvs KVstore,
	dir string,
	Addr string,
	port int,
	transporterPrefix string,
	transporterTimeout time.Duration,
	pulse time.Duration,
) (rkv *Raftkv, err error) {
	connectionString := fmt.Sprintf("%s:%d", Addr, port)
	rkv = &Raftkv{
		peers:    peers,
		kvs:      kvs,
		dataDir:  dir,
		router:   mux.NewRouter(),
		httpAddr: connectionString,
		port:     port,
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// Clear old cluster's configuration
	if len(rkv.peers) > 0 {
		if err = os.RemoveAll(path.Join(rkv.dataDir, "conf")); err != nil {
			return nil, err
		}
		if err = os.RemoveAll(path.Join(rkv.dataDir, "log")); err != nil {
			return nil, err
		}
		if err = os.RemoveAll(path.Join(rkv.dataDir, "snapshot")); err != nil {
			return nil, err
		}
	}

	transporter := raft.NewHTTPTransporter(transporterPrefix, transporterTimeout)
	rkv.server, err = raft.NewServer(connectionString, dir, transporter, nil, rkv.kvs, connectionString)
	transporter.Install(rkv.server, rkv)
	if err = rkv.server.Start(); err != nil {
		return nil, err
	}

	if pulse > 0 {
		rkv.server.SetHeartbeatInterval(pulse)
		rkv.server.SetElectionTimeout(pulse * 5)
	}

	rkv.router.HandleFunc("/raftkv_join", rkv.joinHandler)
	rkv.router.HandleFunc("/raftkv_leave", rkv.leaveHandler)
	rkv.router.HandleFunc("/raftkv_put", rkv.redirectedPut)
	rkv.router.HandleFunc("/raftkv_del", rkv.redirectedDel)
	rkv.router.HandleFunc("/raftkv_get", rkv.redirectedGet)

	if len(rkv.peers) > 0 {
		// fmt.Println(peers)
		err := rkv.Join(rkv.peers)
		if err != nil {
			logger.Println(err)
			// if cannot join clusters, joins itself
			logger.Printf("i am %s, i join myself\n", rkv.server.Name())
			_, err = rkv.server.Do(&raft.DefaultJoinCommand{
				Name:             rkv.server.Name(),
				ConnectionString: connectionString,
			})
			if err != nil {
				return nil, err
			}
		}
	} else if rkv.server.IsLogEmpty() {
		// Initialize the server by joining itself
		_, err = rkv.server.Do(&raft.DefaultJoinCommand{
			Name:             rkv.server.Name(),
			ConnectionString: connectionString,
		})
		if err != nil {
			return nil, err
		}
	}

	return rkv, nil
}

// Leader returns the server's leader
func (rkv *Raftkv) Leader() string {
	return rkv.server.Leader()
}

// Name returns the server's name
func (rkv *Raftkv) Name() string {
	return rkv.server.Name()
}

// Join an existing cluster
func (rkv *Raftkv) Join(peers []string) (e error) {
	command := &raft.DefaultJoinCommand{
		Name:             rkv.server.Name(),
		ConnectionString: rkv.httpAddr,
	}

	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return err
	}

	for _, peer := range peers {
		logger.Printf("i am %s: joining %s\n", rkv.server.Name(), peer)
		if peer == rkv.httpAddr {
			continue
		}
		target := fmt.Sprintf("%s/raftkv_join", peer)
		_, err := postAndError(target, "application/json", &b)
		if err != nil {
			e = err
			continue
		} else {
			return nil
		}
	}

	return e
}

func postAndError(target string, contentType string, b *bytes.Buffer) ([]byte, error) {
	resp, err := http.Post(target, contentType, b)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	reply, _ := ioutil.ReadAll(resp.Body)
	statusCode := resp.StatusCode
	if statusCode != http.StatusOK {
		return nil, errors.New(string(reply))
	}
	return reply, nil
}

// Leave make rkv leaves the cluster
func (rkv *Raftkv) Leave() error {
	logger.Println(rkv.server.Name(), " is leaving")
	command := &raft.DefaultLeaveCommand{
		Name: rkv.server.Name(),
	}
	if _, err := rkv.server.Do(command); err != nil {
		if err == raft.NotLeaderError {
			_, err = rkv.redirectToLeader(rkv.server.Leader(), "raftkv_leave", command)
			return err
		}
		return err
	}
	return nil
}

// HandleFunc a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (rkv *Raftkv) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	rkv.router.HandleFunc(pattern, handler)
}

// TODO: refactor these functions?
func (rkv *Raftkv) joinHandler(w http.ResponseWriter, req *http.Request) {
	logger.Println("some body wanna join me " + rkv.server.Name())
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := rkv.server.Do(command); err != nil {
		switch err {
		case raft.NotLeaderError:
			if _, err = rkv.redirectToLeader(rkv.server.Leader(), "raftkv_join", command); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (rkv *Raftkv) leaveHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultLeaveCommand{}
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := rkv.server.Do(command); err != nil {
		switch err {
		case raft.NotLeaderError:
			rkv.redirectToLeader(rkv.server.Leader(), "raftkv_join", command)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (rkv *Raftkv) redirectedPut(w http.ResponseWriter, req *http.Request) {
	command := &putCommand{}
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// the Do(Command) function is too slow,
	// so I have to use goroutine
	if err := rkv.kvs.Put(command.Key, command.Val); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go rkv.server.Do(command)
}

func (rkv *Raftkv) redirectedDel(w http.ResponseWriter, req *http.Request) {
	command := &delCommand{}
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := rkv.kvs.Delete(command.Key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go rkv.server.Do(command)
}

func (rkv *Raftkv) redirectedGet(w http.ResponseWriter, req *http.Request) {
	command := &getCommand{}
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// logger.Println("incoming getcommand: ", command)
	val, err := rkv.kvs.Get(command.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(val)
}

func (rkv *Raftkv) redirectToLeader(leader string, op string, command raft.Command) ([]byte, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return nil, err
	}

	reply, err := postAndError(fmt.Sprintf("%s/%s", rkv.server.Leader(), op), "application/json", &b)
	if err != nil {
		return nil, err
	}
	// val, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// Get gets a value by key
func (rkv *Raftkv) Get(key []byte) ([]byte, error) {
	getCmd := newGetCommand(key)
	return rkv.redirectToLeader(rkv.server.Leader(), "raftkv_get", getCmd)
}

// Put puts a key-value pair, it overwrites the old one.
func (rkv *Raftkv) Put(key, val []byte) error {
	putCmd := newPutCommand(key, val)
	_, err := rkv.redirectToLeader(rkv.server.Leader(), "raftkv_put", putCmd)
	return err
}

// Del deletes a key-value pair
func (rkv *Raftkv) Del(key []byte) error {
	delCmd := newDelCommand(key)
	_, err := rkv.redirectToLeader(rkv.server.Leader(), "raftkv_del", delCmd)
	return err
}

// ListenAndServe starts the httpServer
func (rkv *Raftkv) ListenAndServe() {
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", rkv.port),
		Handler: rkv.router,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		fmt.Printf("raftkv listen and serve error: %s\n", err)
	}
}
