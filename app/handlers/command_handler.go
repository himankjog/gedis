package handlers

import (
	"fmt"
	"os"
	"path/filepath"
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
	commandRegistry *CommandRegistry
	ctx             *context.Context
}

type CommandHandlerFunc func(*CommandHandler, []constants.DataRepr) []constants.DataRepr
type CommandRegistry map[string]CommandHandlerFunc

func InitCommandHandler(ctx *context.Context) *CommandHandler {
	cmdRegistry := make(CommandRegistry)
	cmdRegistry[constants.PING_COMMAND] = handlePingCommand
	cmdRegistry[constants.ECHO_COMMAND] = handleEchoCommand
	cmdRegistry[constants.GET_COMMAND] = handleGetCommand
	cmdRegistry[constants.SET_COMMAND] = handleSetCommand
	cmdRegistry[constants.INFO_COMMAND] = handleInfoCommand
	cmdRegistry[constants.REPLCONF_COMMAND] = handleReplconfCommand
	cmdRegistry[constants.PSYNC_COMMAND] = handlePsyncCommand

	// Sub-commands
	cmdRegistry[constants.SET_PX_COMMAND] = handleSetPxCommand

	commandHandler := CommandHandler{
		commandRegistry: &cmdRegistry,
		ctx:             ctx,
	}
	return &commandHandler
}

func (h *CommandHandler) ExecuteCommand(cmd string, args []constants.DataRepr) []constants.DataRepr {
	commandName := strings.ToUpper(cmd)
	commandHandler, commandHandlerPresent := (*h.commandRegistry)[commandName]
	if !commandHandlerPresent {
		errMessage := fmt.Sprintf("Command handler not present for command: %s", commandName)
		h.ctx.Logger.Println(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	h.ctx.Logger.Printf("Handling command: %s", cmd)
	return commandHandler(h, args)
}

func handlePingCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	if len(args) > 0 {
		h.ctx.Logger.Printf("PING command doesn't expects any arguments")
	}
	response := utils.CreateStringResponse(constants.PONG_RESPONSE)
	return []constants.DataRepr{response}
}

func handleEchoCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("ECHO command expects %d variables but %d given", 1, len(args))
		h.ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}

	return args
}

func handleGetCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("GET command expects %d variables but %d given", 1, len(args))
		h.ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	key := string(args[0].Data)
	value, valueExists := persistence.Fetch(key)

	if !valueExists {
		h.ctx.Logger.Printf("Unable to GET value for key %s", key)
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	h.ctx.Logger.Printf("For key: %s, fetched value: %s", key, value)
	decodedValue, _ := parser.Decode([]byte(value))
	return []constants.DataRepr{decodedValue}
}

func handleSetCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	if len(args) < 2 {
		errMessage := fmt.Sprintf("SET command expects >%d variables but %d given", 1, len(args))
		h.ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	if len(args) > 2 {
		sub_command := fmt.Sprintf(constants.SUB_COMMAND_FORMAT, constants.SET_COMMAND, string(args[2].Data))
		return h.ExecuteCommand(sub_command, args)
	}
	key := string(args[0].Data)
	value := parser.Encode(args[1])
	err := persistence.Persist(key, string(value), persistence.SetOptions{})

	if err != nil {
		h.ctx.Logger.Printf("Error while handling SET command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	h.ctx.Logger.Printf("Successfully persisted data: '%s'  against key: %s", value, key)
	return []constants.DataRepr{utils.CreateStringResponse("OK")}
}

func handleInfoCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	response := []byte(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		h.ctx.ServerInstance.ReplicationConfig.Role,
		h.ctx.ServerInstance.ReplicationConfig.MasterReplId,
		h.ctx.ServerInstance.ReplicationConfig.MasterReplOffset,
	))
	return []constants.DataRepr{utils.CreateBulkResponse(string(response))}
}

func handleReplconfCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	// Todo: Handle command parameters
	return []constants.DataRepr{utils.CreateStringResponse("OK")}
}

func handlePsyncCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	// TODO: Parse arguments to fetch replId and offset
	responseData := fmt.Sprintf("FULLRESYNC %s %d",
		h.ctx.ServerInstance.ReplicationConfig.MasterReplId, h.ctx.ServerInstance.ReplicationConfig.MasterReplOffset)
	responseDataList := []constants.DataRepr{utils.CreateStringResponse(responseData)}
	// TODO: Get file path from server
	currDir, _ := os.Getwd()
	rdbFilePath := filepath.Join(currDir, "app", "persistence", "storage", "empty_hex.rdb")
	binaryDecodedDataFromFile, err := utils.ReadHexFileToBinary(rdbFilePath)

	if err != nil {
		h.ctx.Logger.Printf("Error while trying to decode data from edb file at path '%s': %v", rdbFilePath, err.Error())
		responseDataList = append(responseDataList, utils.CreateErrorResponse(err.Error()))
		return responseDataList
	}
	responseDataList = append(responseDataList, utils.CreateRdbFileResponse(binaryDecodedDataFromFile))

	return responseDataList
}

// Sub-command handler space

func handleSetPxCommand(h *CommandHandler, args []constants.DataRepr) []constants.DataRepr {
	if len(args) < 4 {
		errMessage := fmt.Sprintf("SET_PX command expects %d variables but %d given", 4, len(args))
		h.ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}

	key := string(args[0].Data)
	value := parser.Encode(args[1])
	expiryDurationInMilli, err := strconv.Atoi(string(args[3].Data))

	if err != nil {
		h.ctx.Logger.Printf("Error while handling SET_PX command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	setOptions := persistence.SetOptions{
		ExpiryDuration: time.Duration(expiryDurationInMilli) * time.Millisecond,
	}
	err = persistence.Persist(key, string(value), setOptions)

	if err != nil {
		h.ctx.Logger.Printf("Error while handling SET_PX command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	h.ctx.Logger.Printf("Successfully persisted data: '%s'  against key: '%s' with millseconds expiry duration '%d'", value, key, expiryDurationInMilli)
	return []constants.DataRepr{utils.CreateStringResponse("OK")}
}
