package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/xichen2020/eventdb/storage"
)

// Paths.
const (
	HealthPath = "/health"
)

// Errors.
var (
	errInvalidRequestMethod = func(allowedHTTPMethods []string) error {
		return xerrors.NewInvalidParamsError(
			fmt.Errorf(
				"invalid request method, allowed methods: %s",
				strings.Join(allowedHTTPMethods, ","),
			),
		)
	}
)

// Response is a JSON HTTP response.
type Response struct {
	State string `json:"state,omitempty"`
	Error string `json:"error,omitempty"`
}

func registerHandlers(mux *http.ServeMux, store storage.Storage) {
	registerHealthHandler(mux)
}

func registerHealthHandler(mux *http.ServeMux) {
	mux.HandleFunc(HealthPath, handle(func(w http.ResponseWriter, r *http.Request) {
		writeSuccessResponse(w)
	}, []string{http.MethodGet}))
}

func handle(next func(w http.ResponseWriter, r *http.Request), allowedHTTPMethods []string) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		isValidMethod := false
		for _, method := range allowedHTTPMethods {
			if strings.ToUpper(r.Method) == method {
				isValidMethod = true
			}
		}
		if !isValidMethod {
			writeErrorResponse(w, errInvalidRequestMethod(allowedHTTPMethods))
			return
		}
		next(w, r)
	})
}

func newSuccessResponse() Response {
	return Response{State: "OK"}
}

func newErrorResponse(err error) Response {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return Response{State: "Error", Error: errStr}
}

func writeSuccessResponse(w http.ResponseWriter) {
	response := newSuccessResponse()
	writeResponse(w, response, nil)
}

func writeErrorResponse(w http.ResponseWriter, err error) {
	writeResponse(w, nil, err)
}

func writeResponse(w http.ResponseWriter, resp interface{}, err error) {
	buf := bytes.NewBuffer(nil)
	if encodeErr := json.NewEncoder(buf).Encode(&resp); encodeErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp = newErrorResponse(encodeErr)
		json.NewEncoder(w).Encode(&resp)
		return
	}

	if err == nil {
		w.WriteHeader(http.StatusOK)
	} else if xerrors.IsInvalidParams(err) {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buf.Bytes())
}
