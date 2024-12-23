package handlers

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

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

	// Sub-commands
	commandRegistry[constants.SET_PX_COMMAND] = handleSetPxCommand
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
	value, valueExists := persistence.Fetch(key)

	if !valueExists {
		log.Printf("Unable to GET value for key %s", key)
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
	if len(args) > 2 {
		sub_command := fmt.Sprintf(constants.SUB_COMMAND_FORMAT, constants.SET_COMMAND, string(args[2].Data))
		return ExecuteCommand(sub_command, args)
	}
	key := string(args[0].Data)
	value := parser.Encode(args[1])
	err := persistence.Persist(key, string(value), persistence.SetOptions{})

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

// Sub-command handler space

func handleSetPxCommand(args []constants.DataRepr) constants.DataRepr {
	if len(args) < 4 {
		errMessage := fmt.Sprintf("SET_PX command expects %d variables but %d given", 4, len(args))
		log.Printf(errMessage)
		return constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		}
	}

	key := string(args[0].Data)
	value := parser.Encode(args[1])
	expiryDurationInMilli, err := strconv.Atoi(string(args[3].Data))

	if err != nil {
		log.Printf("Error while handling SET_PX command: %v", err.Error())
		return constants.DataRepr{
			Type:  constants.BULK,
			Data:  nil,
			Array: nil,
		}
	}
	setOptions := persistence.SetOptions{
		ExpiryDuration: time.Duration(expiryDurationInMilli) * time.Millisecond,
	}
	err = persistence.Persist(key, string(value), setOptions)

	if err != nil {
		log.Printf("Error while handling SET_PX command: %v", err.Error())
		return constants.DataRepr{
			Type:  constants.BULK,
			Data:  nil,
			Array: nil,
		}
	}
	log.Printf("Successfully persisted data: '%s'  against key: '%s' with millseconds expiry duration '%d'", value, key, expiryDurationInMilli)
	return constants.DataRepr{
		Type:  constants.STRING,
		Data:  []byte("OK"),
		Array: nil,
	}
}
