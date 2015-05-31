package raftkv

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

var testPeers = []string{
	"http://127.0.0.1:8787",
	"http://127.0.0.1:8788",
	"http://127.0.0.1:8789",
}

func TestMultiRaftkv(t *testing.T) {
	defer removeDir()
	os.Mkdir("./raft1", 0700)
	os.Mkdir("./raft2", 0700)
	os.Mkdir("./raft3", 0700)
	fmt.Println("testing multi raftkv")
	// creating new leveldb kvstore
	kv1, _ := NewLevelDB("./leveldb1")
	kv2, _ := NewLevelDB("./leveldb2")
	kv3, _ := NewLevelDB("./leveldb3")

	rkv1, err := NewRaftkv(
		testPeers,
		kv1,
		"./raft1",
		"http://127.0.0.1",
		8787,
		"/raft",
		500*time.Millisecond,
		0,
	)

	if err != nil {
		t.Error(err)
	}
	go listenAndServe(8787, rkv1.router)
	time.Sleep(500 * time.Millisecond)

	rkv2, err := NewRaftkv(
		testPeers,
		kv2,
		"./raft2",
		"http://127.0.0.1",
		8788,
		"/raft",
		500*time.Millisecond,
		0,
	)

	if err != nil {
		t.Error(err)
	}
	go listenAndServe(8788, rkv2.router)

	time.Sleep(300 * time.Millisecond)

	rkv3, err := NewRaftkv(
		testPeers,
		kv3,
		"./raft3",
		"http://127.0.0.1",
		8789,
		"/raft",
		500*time.Millisecond,
		0,
	)

	go listenAndServe(8789, rkv3.router)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(500 * time.Millisecond)
	if err = rkv1.AddPeers([]string{
		"http://127.0.0.1:8788",
		"http://127.0.0.1:8789",
	}); err != nil {
		t.Error(err)
	}

	// fmt.Println("this is the leader: ", rkv1.server.Leader())

	// time.Sleep(5 * time.Second)

	if err = rkv2.Put([]byte("test_key1"), []byte("test_val1")); err != nil {
		t.Error(err)
	}
	if err = rkv3.Put([]byte("test_key2"), []byte("test_val3")); err != nil {
		t.Error(err)
	}
	// rkv1 Get
	val, err := rkv1.Get([]byte("test_key1"))
	if err != nil {
		t.Error(err.Error())
	}
	if !bytes.Equal(val, []byte("test_val1")) {
		t.Errorf("expected %s, get %s", []byte("test_val1"), val)
	}

	// rkv3 Get
	val, err = rkv3.Get([]byte("test_key1"))
	if err != nil {
		t.Error(err.Error())
	}
	if !bytes.Equal(val, []byte("test_val1")) {
		t.Errorf("expected %s, get %s", []byte("test_val1"), val)
	}

	if err = rkv3.Del([]byte("test_key2")); err != nil {
		t.Error(err)
	}

	if v, err := rkv2.Get([]byte("test_key2")); err == nil {
		logger.Println(v)
		t.Error("Error should not be nil")
	} else {
		logger.Println("expected error: ", err)
	}

}

func removeDir() {
	os.RemoveAll("./raft1")
	os.RemoveAll("./leveldb1")
	os.RemoveAll("./raft2")
	os.RemoveAll("./leveldb2")
	os.RemoveAll("./raft3")
	os.RemoveAll("./leveldb3")
}

func listenAndServe(port int, r http.Handler) {
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		fmt.Printf("raftkv listen and serve error: %s\n", err)
	}
}
