package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/xichen2020/eventdb/server/http/handlers"
)

const (
	uriScheme = "http://"
)

type httpClient struct {
	client *http.Client

	healthURL string
	writeURL  string
	queryURL  string
}

func newHTTPClient(serverHostPort string) httpClient {
	shp := strings.TrimRight(serverHostPort, "/")
	return httpClient{
		client:    http.DefaultClient,
		healthURL: uriScheme + shp + handlers.HealthPath,
		writeURL:  uriScheme + shp + handlers.WritePath,
		queryURL:  uriScheme + shp + handlers.QueryPath,
	}
}

// returns true if the server is healthy
func (c httpClient) serverIsHealthy() bool {
	req, err := http.NewRequest(http.MethodGet, c.healthURL, nil)
	if err != nil {
		return false
	}
	_, err = c.do(req)
	return err == nil
}

func (c httpClient) write(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.writeURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	_, err = c.do(req)
	return err
}

func (c httpClient) queryRaw(queryStr []byte) (rawQueryResults, error) {
	var results rawQueryResults
	if err := c.doQuery(queryStr, &results); err != nil {
		return rawQueryResults{}, err
	}
	return results, nil
}

func (c httpClient) queryTimeBucket(queryStr []byte) (timeBucketQueryResults, error) {
	var results timeBucketQueryResults
	if err := c.doQuery(queryStr, &results); err != nil {
		return timeBucketQueryResults{}, err
	}
	return results, nil
}

func (c httpClient) doQuery(queryStr []byte, res interface{}) error {
	req, err := http.NewRequest(http.MethodPost, c.queryURL, bytes.NewReader(queryStr))
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	err = json.Unmarshal(resp, res)
	if err != nil {
		return fmt.Errorf("unable to unmarshal response: %v", err)
	}
	return nil
}

func (c httpClient) do(req *http.Request) ([]byte, error) {
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
