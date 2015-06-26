package helper

import (
	"encoding/json"
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
