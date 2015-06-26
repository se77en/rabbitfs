package StoreServer

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/lilwulin/rabbitfs/log"
)

func init() {
	ss, _ := NewStoreServer("./TestDir", 0.4, "127.0.0.1:8787", 10*time.Second)
	go ss.ListenAndServe()
}

func TestStoreServer(t *testing.T) {
	log.Errorln("hayhayhay")
	var b bytes.Buffer
	_, err := postAndError("http://127.0.0.1:8787/vol/1", "application/json", &b)
	if err != nil {
		t.Error(err)
	}
	if _, err := os.Stat("./TestDir/1.vol"); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat("./TestDir/needle_map_vol1"); err != nil {
		t.Error(err)
	}
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
