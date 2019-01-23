package integration

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
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
	return c.post(c.writeURL, data)
}

func (c client) query(data []byte) error {
	return c.post(c.queryURL, data)
}

func (c client) post(url string, data []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("received '%d' status code: %s", resp.StatusCode, string(data))
}
