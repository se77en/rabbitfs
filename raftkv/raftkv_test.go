package raftkv

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lilwulin/rabbitfs/helper"
	"github.com/visionmedia/go-bench"
)

//TODO: refactor these test

var testPeers = []string{
	"http://127.0.0.1:8787",
	"http://127.0.0.1:8788",
	"http://127.0.0.1:8789",
}

func TestMultiRaftkv(t *testing.T) {
	defer helper.RemoveDirs(
		"./raft1", "./leveldb1",
		"./raft2", "./leveldb2",
		"./raft3", "./leveldb3",
		"./raft4", "./leveldb4",
	)
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
	go rkv1.ListenAndServe()
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
	go rkv2.ListenAndServe()

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
	if err != nil {
		t.Error(err)
	}
	go rkv3.ListenAndServe()

	time.Sleep(200 * time.Millisecond)

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

	// Test Join cluster
	kv4, _ := NewLevelDB("./leveldb4")
	rkv4, err := NewRaftkv(
		testPeers,
		kv4,
		"./raft4",
		"http://127.0.0.1",
		8790,
		"/raft",
		500*time.Millisecond,
		0,
	)
	if err != nil {
		t.Error(err)
	}
	go rkv4.ListenAndServe()
	rkv4.Join(testPeers)
	time.Sleep(1000 * time.Millisecond)
	if val, err = rkv4.Get([]byte("test_key1")); err != nil {
		t.Error(err)
	} else {
		logger.Println("get val from rkv4: ", string(val))
	}
	rkv4.Leave()
	time.Sleep(1 * time.Second)
}

func BenchmarkLevelDBKV(b *testing.B) {
	defer helper.RemoveDirs(
		"./raft1", "./leveldb1",
		"./raft2", "./leveldb2",
		"./raft3", "./leveldb3",
		"./raft4", "./leveldb4",
	)
	os.Mkdir("./raft1", 0700)
	os.Mkdir("./raft2", 0700)
	os.Mkdir("./raft3", 0700)
	fmt.Println("testing multi raftkv")
	// creating new leveldb kvstore
	kv1, _ := NewLevelDB("./leveldb1")
	kv2, _ := NewLevelDB("./leveldb2")
	kv3, _ := NewLevelDB("./leveldb3")

	_, _ = NewRaftkv(
		testPeers,
		kv1,
		"./raft1",
		"http://127.0.0.1",
		8787,
		"/raft",
		500*time.Millisecond,
		0,
	)

	rkv2, _ := NewRaftkv(
		testPeers,
		kv2,
		"./raft2",
		"http://127.0.0.1",
		8788,
		"/raft",
		500*time.Millisecond,
		0,
	)

	_, _ = NewRaftkv(
		testPeers,
		kv3,
		"./raft3",
		"http://127.0.0.1",
		8789,
		"/raft",
		500*time.Millisecond,
		0,
	)

	ops := 1000
	ben := bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 5000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 10000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 50000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 100000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 100000
	ben = bench.Start("RAFTKV-GET")
	for i := 0; i < ops; i++ {
		_, _ = rkv2.Get([]byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)
}

func BenchmarkMemKV(b *testing.B) {
	defer helper.RemoveDirs(
		"./raft1",
		"./raft2",
		"./raft3",
	)
	os.Mkdir("./raft1", 0700)
	os.Mkdir("./raft2", 0700)
	os.Mkdir("./raft3", 0700)
	fmt.Println("testing multi raftkv")
	// creating new leveldb kvstore
	kv1 := NewMem()
	kv2 := NewMem()
	kv3 := NewMem()

	_, _ = NewRaftkv(
		testPeers,
		kv1,
		"./raft1",
		"http://127.0.0.1",
		8787,
		"/raft",
		500*time.Millisecond,
		0,
	)

	rkv2, _ := NewRaftkv(
		testPeers,
		kv2,
		"./raft2",
		"http://127.0.0.1",
		8788,
		"/raft",
		500*time.Millisecond,
		0,
	)

	_, _ = NewRaftkv(
		testPeers,
		kv3,
		"./raft3",
		"http://127.0.0.1",
		8789,
		"/raft",
		500*time.Millisecond,
		0,
	)

	ops := 1000
	ben := bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 5000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 10000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 50000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 100000
	ben = bench.Start("RAFTKV-PUT")
	for i := 0; i < ops; i++ {
		_ = rkv2.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)

	ops = 100000
	ben = bench.Start("RAFTKV-GET")
	for i := 0; i < ops; i++ {
		_, _ = rkv2.Get([]byte(fmt.Sprintf("%d", i)))
	}
	ben.End(ops)
}
