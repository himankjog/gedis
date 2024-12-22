package handlers

import (
	"fmt"
	"log"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

type CommandHandler func([]constants.DataRepr) constants.DataRepr

type CommandRegistry map[string]CommandHandler

var commandRegistry CommandRegistry

func init() {
	commandRegistry = make(CommandRegistry)
	commandRegistry[constants.PING_COMMAND] = handlePingCommand
	commandRegistry[constants.ECHO_COMMAND] = handleEchoCommand
	commandRegistry[constants.GET_COMMAND] = handleGetCommand
	commandRegistry[constants.SET_COMMAND] = handleSetCommand
}

func ExecuteCommand(cmd string, args []constants.DataRepr) constants.DataRepr {
	commandName := strings.ToUpper(cmd)
	commandHandler, commandHandlerPresent := commandRegistry[commandName]

	if !commandHandlerPresent {
		errMessage := fmt.Sprintf("Command handler not present for command: %s", commandName)
		log.Printf(errMessage)
		return constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		}
	}
	log.Printf("Handling command: %s", cmd)
	return commandHandler(args)
}

func handlePingCommand(args []constants.DataRepr) constants.DataRepr {
	if len(args) > 0 {
		log.Printf("PING command doesn't expects any arguments")
	}
	return constants.DataRepr{
		Type:  constants.STRING,
		Data:  []byte(constants.PONG_RESPONSE),
		Array: nil,
	}
}

func handleEchoCommand(args []constants.DataRepr) constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("ECHO command expects %d variables but %d given", 1, len(args))
		log.Printf(errMessage)
		return constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		}
	}

	return args[0]
}

func handleGetCommand(args []constants.DataRepr) constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("GET command expects %d variables but %d given", 1, len(args))
		log.Printf(errMessage)
		return constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		}
	}
	key := string(args[0].Data)
	value, err := persistence.Fetch(key)

	if err != nil {
		log.Printf("Error while handling GET command: %v", err.Error())
		return constants.DataRepr{
			Type:  constants.BULK,
			Data:  nil,
			Array: nil,
		}
	}
	log.Printf("For key: %s, fetched value: %s", key, value)
	decodedValue, _ := parser.Decode([]byte(value))
	return decodedValue
}

func handleSetCommand(args []constants.DataRepr) constants.DataRepr {
	if len(args) < 2 {
		errMessage := fmt.Sprintf("SET command expects >%d variables but %d given", 1, len(args))
		log.Printf(errMessage)
		return constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		}
	}
	key := string(args[0].Data)
	value := parser.Encode(args[1])

	err := persistence.Persist(key, string(value))

	if err != nil {
		log.Printf("Error while handling SET command: %v", err.Error())
		return constants.DataRepr{
			Type:  constants.BULK,
			Data:  nil,
			Array: nil,
		}
	}
	log.Printf("Successfully persisted data: '%s'  against key: %s", value, key)
	return constants.DataRepr{
		Type:  constants.STRING,
		Data:  []byte("OK"),
		Array: nil,
	}
}
