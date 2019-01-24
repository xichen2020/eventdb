package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/server/http/handlers"
)

const (
	uriScheme = "http://"
)

type client struct {
	client *http.Client

	healthURL string
	writeURL  string
	queryURL  string
}

func newClient(serverHostPort string) client {
	shp := strings.TrimRight(serverHostPort, "/")
	return client{
		client:    http.DefaultClient,
		healthURL: uriScheme + shp + handlers.HealthPath,
		writeURL:  uriScheme + shp + handlers.WritePath,
		queryURL:  uriScheme + shp + handlers.QueryPath,
	}
}

// returns true if the server is healthy
func (c client) serverIsHealthy() bool {
	req, err := http.NewRequest(http.MethodGet, c.healthURL, nil)
	if err != nil {
		return false
	}
	_, err = c.do(req)
	return err == nil
}

func (c client) write(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.writeURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	_, err = c.do(req)
	return err
}

func (c client) query(data []byte) ([]query.RawResult, error) {
	req, err := http.NewRequest(http.MethodPost, c.queryURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
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

func (c client) do(req *http.Request) ([]byte, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return data, nil
	}
	return nil, fmt.Errorf("received '%d' status code: %s", resp.StatusCode, data)
}
