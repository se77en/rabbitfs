package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/lilwulin/rabbitfs/helper"
	"github.com/visionmedia/go-bench"
)

var (
	testAssignFileIDStr string
	testVolIP           string
)

// TODO: MAKE TESTS MORE ROBUST

func TestRunServer(t *testing.T) {
	rand.Seed(time.Now().Unix())
	dir1, err := NewDirectory("./TestDir1", "127.0.0.1:9331", "", 5*time.Second, 500, 500*time.Millisecond, 0, 0)
	if err != nil {
		panic(err)
	}
	go dir1.ListenAndServe()
	dir2, err := NewDirectory("./TestDir2", "127.0.0.1:9332", "", 5*time.Second, 500, 500*time.Millisecond, 0, 0)
	if err != nil {
		panic(err)
	}
	go dir2.ListenAndServe()
	dir3, err := NewDirectory("./TestDir3", "127.0.0.1:9333", "", 5*time.Second, 500, 500*time.Millisecond, 0, 0)
	if err != nil {
		panic(err)
	}
	go dir3.ListenAndServe()
	time.Sleep(1 * time.Second)
	ss1, err := NewStoreServer("./TestStore1", "./TestStore1", 0.4, "127.0.0.1:8787", 10*time.Second)
	if err != nil {
		panic(err)
	}
	go ss1.ListenAndServe()
	ss2, err := NewStoreServer("./TestStore2", "./TestStore2", 0.4, "127.0.0.1:8788", 10*time.Second)
	if err != nil {
		panic(err)
	}
	go ss2.ListenAndServe()
	ss3, err := NewStoreServer("./TestStore3", "./TestStore3", 0.4, "127.0.0.1:8789", 10*time.Second)
	if err != nil {
		panic(err)
	}
	go ss3.ListenAndServe()
}

func TestCreateVolume(t *testing.T) {
	volidip := &VolumeIDIP{}
	for {
		var b bytes.Buffer
		resp, err := postAndError("http://127.0.0.1:9333/vol/create?replication=2", "application/json", &b)
		if err != nil {
			fmt.Printf("store waiting for polling,expecting to get error:\n %s\n", err.Error())
		} else {
			json.Unmarshal(resp, volidip)
			break
		}
		time.Sleep(1 * time.Second)
	}
	if len(volidip.IP) != 2 {
		t.Error("the replication count should be 2 but get ", len(volidip.IP))
	}
	time.Sleep(5 * time.Second)
}

func TestAssign(t *testing.T) {
	resp, err := http.Get("http://127.0.0.1:9333/dir/assign?replication=2")
	if err != nil {
		t.Error(err)
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("get assign fid: ", string(bytes))
	a := assignFileIDResult{}
	if err = json.Unmarshal(bytes, &a); err != nil {
		t.Error(err)
	}
	testAssignFileIDStr = a.FID
	testVolIP = a.VolIP
}

func TestUpload(t *testing.T) {
	filepath := "./TestStore1/InputData/Massimo.jpg"
	f1, err := os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		t.Error(err)
	}
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	fw, err := w.CreateFormFile("test_file", filepath)
	if err != nil {
		return
	}
	if _, err = io.Copy(fw, f1); err != nil {
		return
	}
	w.Close()
	_, err = postAndError(fmt.Sprintf("http://%s/%s", testVolIP, testAssignFileIDStr), w.FormDataContentType(), &b)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestGetFile(t *testing.T) {
	getAddr := fmt.Sprintf("http://%s/%s", testVolIP, testAssignFileIDStr)
	fmt.Println(getAddr)
	resp, err := http.Get(getAddr)
	if err != nil {
		t.Error(err)
	}
	filepath := "./TestStore1/OutputData/Massimo.jpg"
	data, err := ioutil.ReadAll(resp.Body)
	if err = ioutil.WriteFile(filepath, data, 0644); err != nil {
		t.Error(err)
	}
}

func TestReplicate(t *testing.T) {
	filesData := [3][]byte{}
	filesData[0], _ = ioutil.ReadFile("./TestStore1/1.vol")
	filesData[1], _ = ioutil.ReadFile("./TestStore2/1.vol")
	filesData[2], _ = ioutil.ReadFile("./TestStore3/1.vol")
	for i := range filesData {
		if len(filesData[i]) == 0 {
			if len(filesData[(i+1)%3]) == 0 {
				t.Errorf("file %d should not be 0", (i+1)%3)
			}
			if len(filesData[(i+2)%3]) == 0 {
				t.Errorf("file %d should not be 0", (i+2)%3)
			}
			if bytes.Compare(filesData[(i+1)%3], filesData[(i+2)%3]) != 0 {
				t.Errorf("replicated volume shoud be the same")
			}
			break
		}
	}
}

func TestDelete(t *testing.T) {
	time.Sleep(20 * time.Second)
	helper.RemoveDirs(
		"./TestDir1/vol.conf.json", "./TestDir2/vol.conf.json", "./TestDir3/vol.conf.json",
		"./TestStore1/needle_map_vol1", "./TestStore1/1.vol", "./TestStore1/volIDIPs.json",
		"./TestStore2/needle_map_vol1", "./TestStore2/1.vol", "./TestStore2/volIDIPs.json",
		"./TestStore3/needle_map_vol1", "./TestStore3/1.vol", "./TestStore3/volIDIPs.json",
	)
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
