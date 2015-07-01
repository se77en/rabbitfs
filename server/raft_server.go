package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"code.google.com/p/log4go"

	"github.com/chrislusf/raft"
	"github.com/gorilla/mux"
)

// RaftServer contains raft server and a KVstore
type RaftServer struct {
	raft.Server
	peers           []string
	dataDir         string
	router          *mux.Router
	httpAddr        string
	port            int
	directoryServer *Directory
}

// NewRaftServer returns a new RaftServer and an error
func NewRaftServer(
	directoryServer *Directory,
	peers []string,
	dir string,
	Addr string,
	router *mux.Router,
	transporterPrefix string,
	transporterTimeout time.Duration,
	pulse time.Duration,
) (rs *RaftServer, err error) {
	for i := range peers {
		if peers[i][:7] != "http://" {
			peers[i] = "http://" + peers[i]
		}
	}
	if Addr[:7] != "http://" {
		Addr = "http://" + Addr
	}
	rs = &RaftServer{
		directoryServer: directoryServer,
		peers:           peers,
		dataDir:         dir,
		router:          router,
		httpAddr:        Addr,
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// Clear old cluster's configuration
	if len(rs.peers) > 0 {
		if err = os.RemoveAll(path.Join(rs.dataDir, "conf")); err != nil {
			return nil, err
		}
		if err = os.RemoveAll(path.Join(rs.dataDir, "log")); err != nil {
			return nil, err
		}
		if err = os.RemoveAll(path.Join(rs.dataDir, "snapshot")); err != nil {
			return nil, err
		}
	}

	transporter := raft.NewHTTPTransporter(transporterPrefix, transporterTimeout)
	rs.Server, err = raft.NewServer(Addr, dir, transporter, nil, directoryServer, Addr)
	transporter.Install(rs.Server, rs)
	if err = rs.Server.Start(); err != nil {
		return nil, err
	}

	if pulse > 0 {
		rs.Server.SetHeartbeatInterval(pulse)
		rs.Server.SetElectionTimeout(pulse * 5)
	}

	rs.router.HandleFunc("/raft_server/join", rs.joinHandler)
	if len(rs.peers) > 1 {
		// fmt.Println(peers)
		err := rs.Join(rs.peers)
		if err != nil {
			// if cannot join clusters, joins itself
			log4go.Info("%s join itself\n", rs.Server.Name())
			_, err = rs.Server.Do(&raft.DefaultJoinCommand{
				Name:             rs.Server.Name(),
				ConnectionString: Addr,
			})
			if err != nil {
				return nil, err
			}
		}
	} else {
		// server joining itself
		_, err = rs.Server.Do(&raft.DefaultJoinCommand{
			Name:             rs.Server.Name(),
			ConnectionString: Addr,
		})
		if err != nil {
			return nil, err
		}
	}

	return rs, nil
}

// Leader returns the server's leader
func (rs *RaftServer) Leader() string {
	return rs.Server.Leader()
}

// Name returns the Server's name
func (rs *RaftServer) Name() string {
	return rs.Server.Name()
}

// Join an existing cluster
func (rs *RaftServer) Join(peers []string) (e error) {
	command := &raft.DefaultJoinCommand{
		Name:             rs.Server.Name(),
		ConnectionString: rs.httpAddr,
	}

	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return err
	}

	for _, peer := range peers {
		log4go.Info("%s is joining %s", rs.Server.Name(), peer)
		if peer == rs.httpAddr {
			continue
		}
		target := fmt.Sprintf("%s/raft_server/join", peer)
		_, err := postAndError(target, "application/json", &b)
		if err != nil {
			log4go.Warn(err.Error())
			e = err
			continue
		} else {
			return nil
		}
	}

	return e
}

// HandleFunc is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (rs *RaftServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	rs.router.HandleFunc(pattern, handler)
}

// TODO: refactor these functions?
func (rs *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := rs.Server.Do(command); err != nil {
		switch err {
		case raft.NotLeaderError:
			if _, err = rs.RedirectToLeader(rs.Server.Leader(), "raft_server/join", command); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (rs *RaftServer) createVolCmdHandler(w http.ResponseWriter, req *http.Request) {
	command := &CreateVolCommand{}
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ret, err := rs.Server.Do(command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	volIDIP := ret.(VolumeIDIP)
	bytes, err := json.Marshal(volIDIP)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(bytes)
}

func (rs *RaftServer) RedirectToLeader(leader string, op string, command raft.Command) ([]byte, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return nil, err
	}

	reply, err := postAndError("http://"+fmt.Sprintf("%s/%s", rs.Server.Leader(), op), "application/json", &b)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}
