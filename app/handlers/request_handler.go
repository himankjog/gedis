package handlers

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"github.com/google/uuid"
)

type RequestHandler struct {
	commandHandler *CommandHandler
	ctx            *context.Context
}

func InitRequestHandler(ctx *context.Context, cmdHandler *CommandHandler) *RequestHandler {
	requestHandler := RequestHandler{
		commandHandler: cmdHandler,
		ctx:            ctx,
	}

	return &requestHandler
}

func (h *RequestHandler) ProcessRequest(request constants.Request) []constants.DataRepr {
	requestData, requestId := request.Data, request.RequestId

	decodedRequestData, err := parser.Decode(requestData)

	if err != nil {
		errMessage := fmt.Sprintf("(%s) Error while trying to decode request with data '%q' : %v", requestId.String(), requestData, err.Error())
		h.ctx.Logger.Println(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}

	return processRequest(h, decodedRequestData, requestId)
}

func processRequest(h *RequestHandler, decodedRequestData constants.DataRepr, requestId uuid.UUID) []constants.DataRepr {
	// Request is always going to be an ARRAY type and first element of array will be a command decoded as a bulk string
	// For example: "PING" becomes *1\r\n$4\r\nPING\r\n
	// "ECHO hey" becomes *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	if decodedRequestData.Type != constants.ARRAY {
		errMessage := fmt.Sprintf("(%s) Unable to extract command from decoded request data: %q", requestId.String(), decodedRequestData.Data)
		h.ctx.Logger.Println(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	command := string(decodedRequestData.Array[0].Data)
	args := decodedRequestData.Array[1:]
	h.ctx.Logger.Printf("(%s) Sending command: %s to command handler", requestId.String(), command)

	response := h.commandHandler.ExecuteCommand(constants.ExecuteCommandRequest{
		Cmd:            command,
		Args:           args,
		RequestId:      requestId,
		DecodedRequest: decodedRequestData,
	})
	h.ctx.Logger.Printf("(%s) Response post processing request: %q", requestId.String(), response)
	return response
}
