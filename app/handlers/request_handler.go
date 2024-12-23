package handlers

import (
	"fmt"
	"log"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
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

func ProcessRequest(request Request) []byte {
	requestData, requestId := request.Data, request.RequestId

	decodedRequestData, err := parser.Decode(requestData)

	if err != nil {
		errMessage := fmt.Sprintf("[%s] Error while trying to decode request with data '%q' : %v", requestId.String(), requestData, err.Error())
		log.Println(errMessage)
		return errorResponse(errMessage)
	}

	return processRequest(decodedRequestData, requestId)
}

func processRequest(decodedRequestData constants.DataRepr, requestId uuid.UUID) []byte {
	// Request is always going to be an ARRAY type and first element of array will be a command decoded as a bulk string
	// For example: "PING" becomes *1\r\n$4\r\nPING\r\n
	// "ECHO hey" becomes *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	if decodedRequestData.Type != constants.ARRAY {
		errMessage := fmt.Sprintf("[%s] Unable to extract command from decoded request data: %q", requestId.String(), decodedRequestData.Data)
		log.Println(errMessage)
		return errorResponse(errMessage)
	}
	command := string(decodedRequestData.Array[0].Data)
	args := decodedRequestData.Array[1:]
	log.Printf("[%s] Sending command: %s to command handler", requestId.String(), command)

	response := ExecuteCommand(command, args)
	log.Printf("[%s] Response post processing request: %q", requestId.String(), response)
	return parser.Encode(response)
}

func errorResponse(errMessage string) []byte {
	return parser.Encode(
		constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		},
	)
}
