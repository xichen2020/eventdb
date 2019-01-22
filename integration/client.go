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
	resp, err := http.Post(c.writeURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return nil
	}
	// TODO get response body
	return fmt.Errorf("received '%d' status code on write", resp.StatusCode)
}

func (c client) query(query query.RawQuery) error {
	data, err := json.Marshal(query)
	if err != nil {
		return err
	}
	resp, err := http.Post(c.queryURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	print(string(data))

	if resp.StatusCode == 200 {
		return nil
	}
	// TODO get response body
	return fmt.Errorf("received '%d' status code on query", resp.StatusCode)
}
