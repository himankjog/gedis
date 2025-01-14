package handlers

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type CommandHandler struct {
	CommandRegistry       CommandRegistry
	ctx                   *context.Context
	connectedReplicaCount int
	notificationHandler   *NotificationHandler
}

type CommandHandlerFunc func(*CommandHandler, []constants.DataRepr) ([]constants.DataRepr, error)
type CommandRegistry map[string]CommandHandlerFunc

func InitCommandHandler(ctx *context.Context, notificationHandler *NotificationHandler) *CommandHandler {
	cmdRegistry := make(CommandRegistry)
	cmdRegistry[constants.PING_COMMAND] = handlePingCommand
	cmdRegistry[constants.ECHO_COMMAND] = handleEchoCommand
	cmdRegistry[constants.GET_COMMAND] = handleGetCommand
	cmdRegistry[constants.SET_COMMAND] = handleSetCommand
	cmdRegistry[constants.INFO_COMMAND] = handleInfoCommand
	cmdRegistry[constants.REPLCONF_COMMAND] = handleReplconfCommand
	cmdRegistry[constants.PSYNC_COMMAND] = handlePsyncCommand
	cmdRegistry[constants.WAIT_COMMAND] = handleWaitCommand

	// Sub-commands
	cmdRegistry[constants.SET_PX_COMMAND] = handleSetPxCommand
	cmdRegistry[constants.REPLCONF_GETACK] = handleReplconfGetackCommand

	commandHandler := CommandHandler{
		CommandRegistry:       cmdRegistry,
		ctx:                   ctx,
		connectedReplicaCount: 0,
	}

	commandHandler.notificationHandler = notificationHandler
	notificationHandler.SubscribeToConnectedReplicasHeartbeatNotification(commandHandler.processConnectedReplicasHeartbeatNotification)
	return &commandHandler
}

func (h *CommandHandler) ExecuteCommand(executeCommandRequest constants.ExecuteCommandRequest) []constants.DataRepr {
	commandName := strings.ToUpper(executeCommandRequest.Cmd)
	commandHandler, commandHandlerPresent := h.CommandRegistry[commandName]
	commandExecutedNotification := constants.CommandExecutedNotification{
		Cmd:            commandName,
		RequestId:      executeCommandRequest.RequestId,
		Args:           executeCommandRequest.Args,
		DecodedRequest: executeCommandRequest.DecodedRequest,
		Success:        true,
	}
	if !commandHandlerPresent {
		errMessage := fmt.Sprintf("Command handler not present for command: %s", commandName)
		h.ctx.Logger.Println(errMessage)
		commandExecutedNotification.Success = false
		h.ctx.CommandExecutedNotificationChan <- commandExecutedNotification
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	h.ctx.Logger.Printf("Handling command: %s", commandName)
	result, err := commandHandler(h, executeCommandRequest.Args)
	if err != nil {
		h.ctx.Logger.Printf("Error while trying to execute command [%s]: %v", commandName, err.Error())
		result = append(result, utils.CreateErrorResponse(err.Error()))
		commandExecutedNotification.Success = false
	}
	commandExecutedNotification.DecodedResponseList = result
	h.ctx.CommandExecutedNotificationChan <- commandExecutedNotification
	h.ctx.Logger.Printf("(%s) Successfully executed command [%s]", commandExecutedNotification.RequestId.String(), commandName)
	return result
}

func (h *CommandHandler) processConnectedReplicasHeartbeatNotification(notification constants.ConnectedReplicaHeartbeatNotification) (bool, error) {
	connectedReplicaCount := notification.ConnectedReplicas
	h.connectedReplicaCount = connectedReplicaCount
	// TODO: Handler error scenarios
	return true, nil
}

// Command handlers
func handlePingCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	if len(args) > 0 {
		h.ctx.Logger.Printf("PING command doesn't expects any arguments")
	}
	response := utils.CreateStringResponse(constants.PONG_RESPONSE)
	return []constants.DataRepr{response}, nil
}

func handleEchoCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("ECHO command expects %d variables but %d given", 1, len(args))
		h.ctx.Logger.Print(errMessage)
		return make([]constants.DataRepr, 0), errors.New(errMessage)
	}

	return args, nil
}

func handleGetCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("GET command expects %d variables but %d given", 1, len(args))
		h.ctx.Logger.Print(errMessage)
		return make([]constants.DataRepr, 0), errors.New(errMessage)
	}
	key := string(args[0].Data)
	value, valueExists := persistence.Fetch(key)

	if !valueExists {
		h.ctx.Logger.Printf("Unable to GET value for key %s", key)
		return []constants.DataRepr{utils.NilBulkStringResponse()}, nil
	}
	h.ctx.Logger.Printf("For key: %s, fetched value: %s", key, value)
	decodedValue, _ := parser.Decode([]byte(value))
	return []constants.DataRepr{decodedValue[0]}, nil
}

func handleSetCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	if len(args) < 2 {
		errMessage := fmt.Sprintf("SET command expects >%d variables but %d given", 1, len(args))
		h.ctx.Logger.Print(errMessage)
		return make([]constants.DataRepr, 0), errors.New(errMessage)
	}
	if len(args) > 2 {
		sub_command := fmt.Sprintf(constants.SUB_COMMAND_FORMAT, constants.SET_COMMAND, string(args[2].Data))
		return h.CommandRegistry[strings.ToUpper(sub_command)](h, args)
	}
	key := string(args[0].Data)
	value := parser.Encode(args[1])
	err := persistence.Persist(key, string(value), persistence.SetOptions{})

	if err != nil {
		h.ctx.Logger.Printf("Error while handling SET command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}, nil
	}
	h.ctx.Logger.Printf("Successfully persisted data: '%s'  against key: %s", value, key)
	return []constants.DataRepr{utils.CreateStringResponse("OK")}, nil
}

func handleInfoCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	response := []byte(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		h.ctx.ServerInstance.ReplicationConfig.Role,
		h.ctx.ServerInstance.ReplicationConfig.MasterReplId,
		h.ctx.ServerInstance.ReplicationConfig.MasterReplOffset,
	))
	return []constants.DataRepr{utils.CreateBulkResponse(string(response))}, nil
}

func handleReplconfCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	firstArg := string(args[0].Data)

	switch firstArg {
	case constants.GETACK:
		sub_command := fmt.Sprintf(constants.SUB_COMMAND_FORMAT, constants.REPLCONF_COMMAND, string(args[0].Data))
		return h.CommandRegistry[strings.ToUpper(sub_command)](h, args[1:])
	default:
		//TODO: Handling listening-port and capa pysnc2 here for now. Need to handle them separately
		return []constants.DataRepr{utils.CreateStringResponse("OK")}, nil
	}
}

func handleWaitCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	return []constants.DataRepr{utils.CreateIntegerResponse(h.connectedReplicaCount)}, nil
}

func handlePsyncCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	// TODO: Parse arguments to fetch replId and offset
	responseData := fmt.Sprintf("%s %s %d", constants.FULLRESYNC_RESPONSE,
		h.ctx.ServerInstance.ReplicationConfig.MasterReplId, h.ctx.ServerInstance.ReplicationConfig.MasterReplOffset)
	responseDataList := []constants.DataRepr{utils.CreateStringResponse(responseData)}
	return responseDataList, nil
}

// Sub-command handler space

func handleSetPxCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	if len(args) < 4 {
		errMessage := fmt.Sprintf("SET_PX command expects %d variables but %d given", 4, len(args))
		h.ctx.Logger.Print(errMessage)
		return make([]constants.DataRepr, 0), errors.New(errMessage)
	}

	key := string(args[0].Data)
	value := parser.Encode(args[1])
	expiryDurationInMilli, err := strconv.Atoi(string(args[3].Data))

	if err != nil {
		h.ctx.Logger.Printf("Error while handling SET_PX command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}, nil
	}
	setOptions := persistence.SetOptions{
		ExpiryDuration: time.Duration(expiryDurationInMilli) * time.Millisecond,
	}
	err = persistence.Persist(key, string(value), setOptions)

	if err != nil {
		h.ctx.Logger.Printf("Error while handling SET_PX command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}, nil
	}
	h.ctx.Logger.Printf("Successfully persisted data: '%s'  against key: '%s' with millseconds expiry duration '%d'", value, key, expiryDurationInMilli)
	return []constants.DataRepr{utils.CreateStringResponse("OK")}, nil
}

func handleReplconfGetackCommand(h *CommandHandler, args []constants.DataRepr) ([]constants.DataRepr, error) {
	return []constants.DataRepr{utils.CreateReplconfAck(args[0].Data, 0)}, nil
}
