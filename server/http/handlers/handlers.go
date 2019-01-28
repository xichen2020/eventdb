package handlers

import (
	"net/http"
)

// A list of HTTP endpoints.
// TODO(xichen): API versioning.
const (
	HealthPath = "/health"
	WritePath  = "/write"
	QueryPath  = "/query"
)

// RegisterService registers handler service.
func RegisterService(mux *http.ServeMux, s Service) {
	mux.HandleFunc(HealthPath, s.Health)
	mux.HandleFunc(WritePath, s.Write)
	mux.HandleFunc(QueryPath, s.Query)
}
