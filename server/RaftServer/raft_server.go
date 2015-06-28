package RaftServer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"github.com/lilwulin/rabbitfs/log"
	"github.com/lilwulin/rabbitfs/server/DirectoryServer"
)

// RaftServer contains raft server and a KVstore
type RaftServer struct {
	peers     []string
	server    raft.Server
	dataDir   string
	router    *mux.Router
	httpAddr  string
	port      int
	directory *DirectoryServer.Directory
}

// NewRaftServer returns a new RaftServer and an error
func NewRaftServer(
	directory *DirectoryServer.Directory,
	peers []string,
	dir string,
	Addr string,
	transporterPrefix string,
	transporterTimeout time.Duration,
	pulse time.Duration,
) (rs *RaftServer, err error) {
	for i := range peers {
		if peers[i][:7] != "http://" {
			peers[i] = "http://" + peers[i]
		}
	}
	rs = &RaftServer{
		directory: directory,
		peers:     peers,
		dataDir:   dir,
		router:    mux.NewRouter(),
		httpAddr:  Addr,
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
	rs.server, err = raft.NewServer(Addr, dir, transporter, nil, directory, Addr)
	transporter.Install(rs.server, rs)
	if err = rs.server.Start(); err != nil {
		return nil, err
	}

	if pulse > 0 {
		rs.server.SetHeartbeatInterval(pulse)
		rs.server.SetElectionTimeout(pulse * 5)
	}

	rs.router.HandleFunc("/raft_server/join", rs.joinHandler)

	if len(rs.peers) > 0 {
		// fmt.Println(peers)
		err := rs.Join(rs.peers)
		if err != nil {
			// if cannot join clusters, joins itself
			log.Infof(0, "i am %s, i join myself\n", rs.server.Name())
			_, err = rs.server.Do(&raft.DefaultJoinCommand{
				Name:             rs.server.Name(),
				ConnectionString: Addr,
			})
			if err != nil {
				return nil, err
			}
		}
	} else if rs.server.IsLogEmpty() {
		// Initialize the server by joining itself
		_, err = rs.server.Do(&raft.DefaultJoinCommand{
			Name:             rs.server.Name(),
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
	return rs.server.Leader()
}

// Name returns the server's name
func (rs *RaftServer) Name() string {
	return rs.server.Name()
}

// Join an existing cluster
func (rs *RaftServer) Join(peers []string) (e error) {
	command := &raft.DefaultJoinCommand{
		Name:             rs.server.Name(),
		ConnectionString: rs.httpAddr,
	}

	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return err
	}

	for _, peer := range peers {
		log.Infoln(0, "i am ", rs.server.Name(), ", joining ", peer)
		if peer == rs.httpAddr {
			continue
		}
		target := fmt.Sprintf("%s/raft_server/join", peer)
		_, err := postAndError(target, "application/json", &b)
		if err != nil {
			fmt.Println(err)
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

// HandleFunc a hack around Gorilla mux not providing the correct net/http
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
	if _, err := rs.server.Do(command); err != nil {
		switch err {
		case raft.NotLeaderError:
			if _, err = rs.RedirectToLeader(rs.server.Leader(), "raft_server/join", command); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (rs *RaftServer) RedirectToLeader(leader string, op string, command raft.Command) ([]byte, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return nil, err
	}

	reply, err := postAndError(fmt.Sprintf("%s/%s", rs.server.Leader(), op), "application/json", &b)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// ListenAndServe starts the httpServer
func (rs *RaftServer) ListenAndServe() {
	connectString := rs.httpAddr
	if connectString[:7] == "http://" {
		connectString = connectString[7:]
	}
	httpServer := &http.Server{
		Addr:    connectString,
		Handler: rs.router,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("raft server listen and serve error: %s\n", err)
	}
}
