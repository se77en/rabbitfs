package server

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

func postAndError(target string, contentType string, b io.Reader) ([]byte, error) {
	resp, err := client.Post(target, contentType, b)
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
