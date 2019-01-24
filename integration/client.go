package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/xichen2020/eventdb/query"
)

type client struct {
	healthURL string
	writeURL  string
	queryURL  string
}

func newClient(serverAddress string) client {
	var (
		protocol = "http://"
		sa       = strings.TrimRight(serverAddress, "/")
	)
	return client{
		healthURL: protocol + sa + "/health",
		writeURL:  protocol + sa + "/write",
		queryURL:  protocol + sa + "/query",
	}
}

// returns true if the server is healthy
func (c client) serverIsHealthy() bool {
	resp, err := http.Get(c.healthURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return true
	}
	return false
}

func (c client) write(data []byte) error {
	_, err := c.post(c.writeURL, data)
	return err
}

func (c client) query(data []byte) ([]query.RawResult, error) {
	resp, err := c.post(c.queryURL, data)
	if err != nil {
		return nil, err
	}
	var result []query.RawResult
	err = json.Unmarshal(resp, &result)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response: %v", err)
	}
	return result, nil
}

func (c client) post(url string, data []byte) ([]byte, error) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return data, nil
	}
	return nil, fmt.Errorf("received '%d' status code: %s", resp.StatusCode, string(data))
}
