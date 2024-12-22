package handlers

import (
	"fmt"
	"log"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
)

type CommandHandler func([]constants.DataRepr) constants.DataRepr

type CommandRegistry map[string]CommandHandler

var commandRegistry CommandRegistry

func init() {
	commandRegistry = make(CommandRegistry)
	commandRegistry[constants.PING_COMMAND] = handlePingCommand
	commandRegistry[constants.ECHO_COMMAND] = handleEchoCommand
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
		log.Printf("PING command doesn't accepts any arguments")
	}
	return constants.DataRepr{
		Type:  constants.STRING,
		Data:  []byte(constants.PONG_RESPONSE),
		Array: nil,
	}
}

func handleEchoCommand(args []constants.DataRepr) constants.DataRepr {
	if len(args) != 1 {
		errMessage := fmt.Sprintf("ECHO command accepts %d variables but %d given", 1, len(args))
		log.Printf(errMessage)
		return constants.DataRepr{
			Type:  constants.ERROR,
			Data:  []byte(errMessage),
			Array: nil,
		}
	}

	return args[0]
}
