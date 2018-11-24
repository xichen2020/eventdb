package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/m3db/m3x/errors"
)

// Response is an HTTP response.
type Response struct {
	State string `json:"state,omitempty"`
	Error string `json:"error,omitempty"`
}

// NewResponse creates a new empty response.
func NewResponse() Response { return Response{} }

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
	} else if errors.IsInvalidParams(err) {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buf.Bytes())
}
