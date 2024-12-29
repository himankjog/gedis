package handlers

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type CommandHandler func([]constants.DataRepr) []constants.DataRepr

type CommandRegistry map[string]CommandHandler

var commandRegistry CommandRegistry

func init() {
	commandRegistry = make(CommandRegistry)
	commandRegistry[constants.PING_COMMAND] = handlePingCommand
	commandRegistry[constants.ECHO_COMMAND] = handleEchoCommand
	commandRegistry[constants.GET_COMMAND] = handleGetCommand
	commandRegistry[constants.SET_COMMAND] = handleSetCommand
	commandRegistry[constants.INFO_COMMAND] = handleInfoCommand
	commandRegistry[constants.REPLCONF_COMMAND] = handleReplconfCommand
	commandRegistry[constants.PSYNC_COMMAND] = handlePsyncCommand

	// Sub-commands
	commandRegistry[constants.SET_PX_COMMAND] = handleSetPxCommand
}

func ExecuteCommand(cmd string, args []constants.DataRepr) []constants.DataRepr {
	commandName := strings.ToUpper(cmd)
	commandHandler, commandHandlerPresent := commandRegistry[commandName]

	if !commandHandlerPresent {
		errMessage := fmt.Sprintf("Command handler not present for command: %s", commandName)
		ctx.Logger.Println(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	ctx.Logger.Printf("Handling command: %s", cmd)
	return commandHandler(args)
}

func handlePingCommand(args []constants.DataRepr) []constants.DataRepr {
	if len(args) > 0 {
		ctx.Logger.Printf("PING command doesn't expects any arguments")
	}
	response := utils.CreateStringResponse(constants.PONG_RESPONSE)
	return []constants.DataRepr{response}
}

func handleEchoCommand(args []constants.DataRepr) []constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("ECHO command expects %d variables but %d given", 1, len(args))
		ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}

	return args
}

func handleGetCommand(args []constants.DataRepr) []constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("GET command expects %d variables but %d given", 1, len(args))
		ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	key := string(args[0].Data)
	value, valueExists := persistence.Fetch(key)

	if !valueExists {
		ctx.Logger.Printf("Unable to GET value for key %s", key)
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	ctx.Logger.Printf("For key: %s, fetched value: %s", key, value)
	decodedValue, _ := parser.Decode([]byte(value))
	return []constants.DataRepr{decodedValue}
}

func handleSetCommand(args []constants.DataRepr) []constants.DataRepr {
	if len(args) < 2 {
		errMessage := fmt.Sprintf("SET command expects >%d variables but %d given", 1, len(args))
		ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}
	if len(args) > 2 {
		sub_command := fmt.Sprintf(constants.SUB_COMMAND_FORMAT, constants.SET_COMMAND, string(args[2].Data))
		return ExecuteCommand(sub_command, args)
	}
	key := string(args[0].Data)
	value := parser.Encode(args[1])
	err := persistence.Persist(key, string(value), persistence.SetOptions{})

	if err != nil {
		ctx.Logger.Printf("Error while handling SET command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	ctx.Logger.Printf("Successfully persisted data: '%s'  against key: %s", value, key)
	return []constants.DataRepr{utils.CreateStringResponse("OK")}
}

func handleInfoCommand(args []constants.DataRepr) []constants.DataRepr {
	response := []byte(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		ctx.ServerInstance.ReplicationConfig.Role,
		ctx.ServerInstance.ReplicationConfig.MasterReplId,
		ctx.ServerInstance.ReplicationConfig.MasterReplOffset,
	))
	return []constants.DataRepr{utils.CreateBulkResponse(string(response))}
}

func handleReplconfCommand(args []constants.DataRepr) []constants.DataRepr {
	// Todo: Handle command parameters
	return []constants.DataRepr{utils.CreateStringResponse("OK")}
}

func handlePsyncCommand(args []constants.DataRepr) []constants.DataRepr {
	// TODO: Parse arguments to fetch replId and offset
	responseData := fmt.Sprintf("FULLRESYNC %s %d",
		ctx.ServerInstance.ReplicationConfig.MasterReplId, ctx.ServerInstance.ReplicationConfig.MasterReplOffset)
	responseDataList := []constants.DataRepr{utils.CreateStringResponse(responseData)}
	// TODO: Get file path from server
	currDir, _ := os.Getwd()
	rdbFilePath := filepath.Join(currDir, "app", "persistence", "storage", "empty_hex.rdb")
	binaryDecodedDataFromFile, err := utils.ReadHexFileToBinary(rdbFilePath)

	if err != nil {
		ctx.Logger.Printf("Error while trying to decode data from edb file at path '%s': %v", rdbFilePath, err.Error())
		responseDataList = append(responseDataList, utils.CreateErrorResponse(err.Error()))
		return responseDataList
	}
	responseDataList = append(responseDataList, utils.CreateRdbFileResponse(binaryDecodedDataFromFile))

	return responseDataList
}

// Sub-command handler space

func handleSetPxCommand(args []constants.DataRepr) []constants.DataRepr {
	if len(args) < 4 {
		errMessage := fmt.Sprintf("SET_PX command expects %d variables but %d given", 4, len(args))
		ctx.Logger.Print(errMessage)
		return []constants.DataRepr{utils.CreateErrorResponse(errMessage)}
	}

	key := string(args[0].Data)
	value := parser.Encode(args[1])
	expiryDurationInMilli, err := strconv.Atoi(string(args[3].Data))

	if err != nil {
		ctx.Logger.Printf("Error while handling SET_PX command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	setOptions := persistence.SetOptions{
		ExpiryDuration: time.Duration(expiryDurationInMilli) * time.Millisecond,
	}
	err = persistence.Persist(key, string(value), setOptions)

	if err != nil {
		ctx.Logger.Printf("Error while handling SET_PX command: %v", err.Error())
		return []constants.DataRepr{utils.NilBulkStringResponse()}
	}
	ctx.Logger.Printf("Successfully persisted data: '%s'  against key: '%s' with millseconds expiry duration '%d'", value, key, expiryDurationInMilli)
	return []constants.DataRepr{utils.CreateStringResponse("OK")}
}
