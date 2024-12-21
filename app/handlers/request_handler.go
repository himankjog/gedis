package handlers

import (
	"errors"

	"github.com/google/uuid"
)

type Request struct {
	Data      []byte
	RequestId uuid.UUID
}

const (
	PONG         = "+PONG\r\n"
	PING_REQUEST = "PING\r\n"
)

func ProcessRequest(request Request) ([]byte, error) {
	// NO-OP
	return make([]byte, 0), errors.New("Requst processor yet to be implemented")
}
