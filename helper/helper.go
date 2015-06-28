package helper

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
)

func RemoveDirs(dirs ...string) {
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
}

func WriteJson(w http.ResponseWriter, obj interface{}, httpStatus int) error {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_, err = w.Write(bytes)
	return err
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

func UInt32ToBytes(b []byte, i uint32) {
	binary.LittleEndian.PutUint32(b, i)
}

func BytesToUInt32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func UInt64ToBytes(b []byte, i uint64) {
	binary.LittleEndian.PutUint64(b, i)
}

func BytesToUInt64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
