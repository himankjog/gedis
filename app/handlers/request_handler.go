package handlers

import (
	"fmt"
	"log"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	requestUtils "github.com/codecrafters-io/redis-starter-go/app/utils/request"
	"github.com/google/uuid"
)

type Request struct {
	Data      []byte
	RequestId uuid.UUID
}

func ProcessRequest(request Request) []constants.DataRepr {
	requestData, requestId := request.Data, request.RequestId

	decodedRequestData, err := parser.Decode(requestData)

	if err != nil {
		errMessage := fmt.Sprintf("[%s] Error while trying to decode request with data '%q' : %v", requestId.String(), requestData, err.Error())
		log.Println(errMessage)
		return []constants.DataRepr{requestUtils.CreateErrorResponse(errMessage)}
	}

	return processRequest(decodedRequestData, requestId)
}

func processRequest(decodedRequestData constants.DataRepr, requestId uuid.UUID) []constants.DataRepr {
	// Request is always going to be an ARRAY type and first element of array will be a command decoded as a bulk string
	// For example: "PING" becomes *1\r\n$4\r\nPING\r\n
	// "ECHO hey" becomes *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	if decodedRequestData.Type != constants.ARRAY {
		errMessage := fmt.Sprintf("[%s] Unable to extract command from decoded request data: %q", requestId.String(), decodedRequestData.Data)
		log.Println(errMessage)
		return []constants.DataRepr{requestUtils.CreateErrorResponse(errMessage)}
	}
	command := string(decodedRequestData.Array[0].Data)
	args := decodedRequestData.Array[1:]
	log.Printf("[%s] Sending command: %s to command handler", requestId.String(), command)

	response := ExecuteCommand(command, args)
	log.Printf("[%s] Response post processing request: %q", requestId.String(), response)
	return response
}
