package integration

import (
	"testing"
)

func TestS(t *testing.T) {
	_, closer := setupDB(t, "config/config.yaml")
	defer closer()
}
