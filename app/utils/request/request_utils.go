package utils

import "github.com/codecrafters-io/redis-starter-go/app/constants"

func CreateRequestForCommand(command string, subCommands ...string) constants.DataRepr {
	requestDataArray := []constants.DataRepr{}

	cmdDataRepr := constants.DataRepr{
		Type: constants.BULK,
		Data: []byte(command),
	}

	requestDataArray = append(requestDataArray, cmdDataRepr)

	for _, subCommand := range subCommands {
		subCommandDataRepr := constants.DataRepr{
			Type: constants.BULK,
			Data: []byte(subCommand),
		}
		requestDataArray = append(requestDataArray, subCommandDataRepr)
	}

	return constants.DataRepr{
		Type:  constants.ARRAY,
		Array: requestDataArray,
	}
}

func CreateStringResponse(responseString string) constants.DataRepr {
	return constants.DataRepr{
		Type: constants.STRING,
		Data: []byte(responseString),
	}
}
