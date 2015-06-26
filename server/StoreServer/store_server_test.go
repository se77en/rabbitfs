package StoreServer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"testing"
	"time"
)

func init() {
	ss, _ := NewStoreServer("./TestDir", 0.4, "127.0.0.1:8787", 10*time.Second)
	go ss.ListenAndServe()
}

func TestCreateVolume(t *testing.T) {
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

func TestUploadFile(t *testing.T) {
	filepath := "./TestDir/InputData/Massimo.jpg"
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
	resp, err := postAndError("http://127.0.0.1:8787/1,1,1", w.FormDataContentType(), &b)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("Get response: \n", string(resp))
}

func TestGetFile(t *testing.T) {
	filepath := "./TestDir/OutputData/Massimo.jpg"
	resp, err := http.Get("http://127.0.0.1:8787/1,1,1")
	if err != nil {
		t.Error(err)
	}
	resp.Header.Get("Content-Disposition")
	data, err := ioutil.ReadAll(resp.Body)
	ioutil.WriteFile(filepath, data, 0644)
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
