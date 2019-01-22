package integration

import (
	"fmt"
	"io"
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

func (c client) write(reader io.Reader) error {
	resp, err := http.Post(c.writeURL, "application/json", reader)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return nil
	}
	return fmt.Errorf("received '%d' status code on write", resp.StatusCode)
}
