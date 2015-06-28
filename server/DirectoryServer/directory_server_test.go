package DirectoryServer

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/visionmedia/go-bench"
)

func init() {
	rand.Seed(time.Now().Unix())
	ms, err := NewDirectory("./TestDir", "127.0.0.1:9333", "", "", 5*time.Second, "raft", 0, 0)
	if err != nil {
		log.Fatal(err)
	}
	go ms.ListenAndServe()
}

func TestMasterServer(t *testing.T) {
	// var b bytes.Buffer
	// resp, err := postAndError("http://127.0.0.1:9333/vol/create?replication=2", "application/json", &b)
	// if err != nil {
	// 	t.Error(err)
	// }
	// fmt.Println(string(resp))
	var b bytes.Buffer
	resp, err := postAndError("http://127.0.0.1:9333/dir/assign?replication=2", "application/json", &b)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(resp))
}

func BenchmarkAssign(b *testing.B) {
	ops := 10000
	ben := bench.Start("Assign")
	for i := 0; i < ops; i++ {
		var bb bytes.Buffer
		_, _ = postAndError("http://127.0.0.1:9333/dir/assign?replication=2", "application/json", &bb)
	}
	ben.End(ops)
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
