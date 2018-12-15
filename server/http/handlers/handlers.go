package handlers

import (
	"net/http"
)

// A list of HTTP endpoints.
// TODO(xichen): API versioning.
const (
	healthPath = "/health"
	writePath  = "/write"
	queryPath  = "/query"
)

// RegisterService registers handler service.
func RegisterService(mux *http.ServeMux, s Service) {
	mux.HandleFunc(healthPath, s.Health)
	mux.HandleFunc(writePath, s.Write)
	mux.HandleFunc(queryPath, s.Query)
}
