package parser

import (
	"fmt"
	"log"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
)

const (
	CRLF = "\r\n"
)

func Encode(data constants.DataRepr) []byte {
	log.Printf("Encoding response from data: %q", data)
	encodedResponse := encode(data)
	return []byte(encodedResponse)
}

func encode(data constants.DataRepr) string {
	switch data.Type {
	case constants.STRING:
		return encodeString(data.Data)
	case constants.BULK:
		return encodeBulkString(data.Data)
	case constants.INTEGER:
		return encodeInteger(data.Data)
	case constants.ERROR:
		return encodeError(data.Data)
	case constants.ARRAY:
		return encodeArray(data.Array)
	default:
		errMessage := fmt.Sprintf("Unsupported data type: %q", data.Type)
		log.Println(errMessage)
		return encodeError([]byte(errMessage))
	}
}

func encodeString(data []byte) string {
	encodedString := string(data)
	log.Printf("Encoded string value as: %s", encodedString)

	return string(constants.STRING) + encodedString + CRLF
}

func encodeBulkString(data []byte) string {
	if data == nil {
		log.Printf("Encoding null bulk string")
		return string(constants.BULK) + strconv.Itoa(-1) + CRLF
	}
	bulkString := string(data)
	bulkStringLength := len(bulkString)
	encodedBulkString := strconv.Itoa(bulkStringLength) + CRLF + bulkString
	log.Printf("Encoded bulk string: %s", encodedBulkString)

	return string(constants.BULK) + encodedBulkString + CRLF
}

func encodeError(data []byte) string {
	encodedErrorString := string(data)
	log.Printf("Encoded error string: %s", encodedErrorString)

	return string(constants.ERROR) + encodedErrorString + CRLF
}

func encodeInteger(data []byte) string {
	integerVal, _ := strconv.Atoi(string(data))
	log.Printf("Encoded integer value as: %d", integerVal)

	return string(constants.INTEGER) + strconv.Itoa(integerVal) + CRLF
}

func encodeArray(dataArray []constants.DataRepr) string {
	arrayLength := len(dataArray)
	encodedArrayString := string(constants.ARRAY) + strconv.Itoa(arrayLength) + CRLF

	for _, data := range dataArray {
		encodedDataString := encode(data)
		encodedArrayString += encodedDataString
	}
	log.Printf("Encoded Array value as: %s", encodedArrayString)

	return encodedArrayString
}