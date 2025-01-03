package utils

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
)

func CreateRequestForCommand(command string, subCommands ...string) constants.DataRepr {
	requestDataArray := []constants.DataRepr{}

	cmdDataRepr := CreateBulkResponse(command)

	requestDataArray = append(requestDataArray, cmdDataRepr)

	for _, subCommand := range subCommands {
		subCommandDataRepr := CreateBulkResponse(subCommand)
		requestDataArray = append(requestDataArray, subCommandDataRepr)
	}

	return CreateArrayDataRepr(requestDataArray)
}

func CreateIntegerResponse(data string) constants.DataRepr {
	return CreateAtomicDataReprFromString(data, constants.INTEGER)
}

func CreateStringResponse(data string) constants.DataRepr {
	return CreateAtomicDataReprFromString(data, constants.STRING)
}

func CreateBulkResponse(data string) constants.DataRepr {
	return CreateAtomicDataReprFromString(data, constants.BULK)
}

func CreateErrorResponse(data string) constants.DataRepr {
	return CreateAtomicDataReprFromString(data, constants.ERROR)
}

func CreateRdbFileResponse(data []byte) constants.DataRepr {
	return CreateAtomicDataReprFromByte(data, constants.RDB_FILE)
}

func CreateAtomicDataReprFromString(data string, dataType constants.DataType) constants.DataRepr {
	return constants.DataRepr{
		Type:  dataType,
		Data:  []byte(data),
		Array: nil,
	}
}

func CreateAtomicDataReprFromByte(data []byte, dataType constants.DataType) constants.DataRepr {
	return constants.DataRepr{
		Type:  dataType,
		Data:  data,
		Array: nil,
	}
}

func CreateArrayDataRepr(dataArray []constants.DataRepr) constants.DataRepr {
	return constants.DataRepr{
		Type:  constants.ARRAY,
		Data:  nil,
		Array: dataArray,
	}
}

func NilBulkStringResponse() constants.DataRepr {
	return constants.DataRepr{
		Type:  constants.BULK,
		Data:  nil,
		Array: nil,
	}
}

func CreateReplconfGetack(replicaOffset int) constants.DataRepr {
	ctx.Logger.Printf("Received offset from master '%d'", replicaOffset)
	return CreateArrayDataRepr([]constants.DataRepr{
		CreateBulkResponse(constants.REPLCONF_COMMAND),
		CreateBulkResponse(constants.GETACK),
		CreateBulkResponse("*"),
	})
}

func CreateReplconfAck(offsetFromMaster []byte, replicaOffset int) constants.DataRepr {
	ctx.Logger.Printf("Received offset from master '%q' and replica offset is '%d'", offsetFromMaster, replicaOffset)
	return CreateArrayDataRepr([]constants.DataRepr{
		CreateBulkResponse(constants.REPLCONF_COMMAND),
		CreateBulkResponse(constants.ACK),
		CreateBulkResponse(strconv.Itoa(replicaOffset)),
	})
}
