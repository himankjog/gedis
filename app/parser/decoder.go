package parser

import (
	"bufio"
	"bytes"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
)

func Decode(data []byte) (constants.DataRepr, error) {
	if len(data) == 0 {
		return constants.DataRepr{}, errors.New("no data provided")
	}
	reader := bufio.NewReader(bytes.NewReader(data))
	return decode(reader)
}

func decode(reader *bufio.Reader) (constants.DataRepr, error) {
	data, _, err := ReadNext(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to start decode: %s", err)
		return constants.DataRepr{}, err
	}
	switch data[0] {
	case constants.INTEGER:
		return decodeInteger(data[1:])
	case constants.STRING:
		return decodeSimpleString(data[1:])
	case constants.BULK:
		bulkStringData, _, err := ReadNext(reader)
		if err != nil {
			ctx.Logger.Printf("Error trying to read bulk string data from decode: %s", err)
			return constants.DataRepr{}, err
		}
		bulkStringData = append(data, bulkStringData...)
		return decodeBulkString(bulkStringData[1:])
	case constants.ERROR:
		return decodeError(data[1:])
	case constants.ARRAY:
		return decodeArray(data[1:], reader)
	default:
		ctx.Logger.Printf("Unsupported data type: %v", data[0])
		return constants.DataRepr{}, errors.New("unsupported data type")
	}
}

func decodeInteger(data []byte) (constants.DataRepr, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	integerData, charactersRead, err := ReadNext(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read integer data in decodeInteger: %s", err)
		return constants.DataRepr{}, errors.New("error parsing integer data")
	}
	integer, err := strconv.Atoi(string(integerData[:charactersRead]))
	if err != nil {
		ctx.Logger.Printf("Error trying to parse integer from integer data: %s", err)
		return constants.DataRepr{}, errors.New("error parsing integer from integer data")
	}
	ctx.Logger.Printf("Parsed integer: %d", integer)
	return constants.DataRepr{
		Type:  constants.INTEGER,
		Data:  integerData[:charactersRead],
		Array: nil,
	}, nil
}

func decodeSimpleString(data []byte) (constants.DataRepr, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	simpleString, charactersRead, err := ReadNext(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read simple string in decodeSimpleString: %s", err)
		return constants.DataRepr{}, err
	}
	ctx.Logger.Printf("Parsed simple string: %s", simpleString)
	return constants.DataRepr{
		Type:  constants.STRING,
		Data:  simpleString[:charactersRead],
		Array: nil,
	}, nil
}

func decodeBulkString(data []byte) (constants.DataRepr, error) {
	bulkStringReader := bufio.NewReader(bytes.NewReader(data))
	bulkStringLengthBytes, charactersRead, err := ReadNext(bulkStringReader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read bulk string length bytes in decodeBulkString: %s", err)
		return constants.DataRepr{}, err
	}
	bulkStringLength, err := strconv.Atoi(string(bulkStringLengthBytes[:charactersRead]))
	if err != nil {
		ctx.Logger.Printf("Error trying to parse bulk string length from bulk string length bytes: %s", err)
		return constants.DataRepr{}, err
	}
	bulkStringBytes := make([]byte, bulkStringLength+2)
	readStringLength, err := bulkStringReader.Read(bulkStringBytes)
	if err != nil {
		ctx.Logger.Printf("Error trying to read bulk string bytes in decodeBulkString: %s", err)
		return constants.DataRepr{}, err
	}
	if readStringLength > bulkStringLength+2 {
		ctx.Logger.Printf("Error: read string length does not match bulk string length")
		return constants.DataRepr{}, errors.New("provided length does not match provided data length")
	}

	// if readStringLength > 2 {
	// 	lastChar := bulkStringBytes[readStringLength-1]
	// 	secondToLastChar := bulkStringBytes[readStringLength-2]
	// 	if secondToLastChar != '\r' || lastChar != '\n' {
	// 		ctx.Logger.Printf("Error: bulk string does not end with CRLF")
	// 		return constants.DataRepr{}, errors.New("bulk string does not end with CRLF")
	// 	}
	// }

	return constants.DataRepr{
		Type:  constants.BULK,
		Data:  bulkStringBytes[:bulkStringLength],
		Array: nil,
	}, nil
}

func decodeError(data []byte) (constants.DataRepr, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	errorData, charactersRead, err := ReadNext(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read error data in decodeError: %s", err)
		return constants.DataRepr{}, err
	}
	ctx.Logger.Printf("Parsed error: %s", errorData)
	return constants.DataRepr{
		Type:  constants.ERROR,
		Data:  errorData[:charactersRead],
		Array: nil,
	}, nil
}

func decodeArray(data []byte, reader *bufio.Reader) (constants.DataRepr, error) {
	arrayReader := bufio.NewReader(bytes.NewReader(data))
	arrayLengthBytes, charactersRead, err := ReadNext(arrayReader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read array length bytes in decodeArray: %s", err)
		return constants.DataRepr{}, err
	}

	arrayLength, err := strconv.Atoi(string(arrayLengthBytes[:charactersRead]))
	if err != nil {
		ctx.Logger.Printf("Error trying to parse array length from array length bytes: %s", err)
		return constants.DataRepr{}, err
	}

	var array []constants.DataRepr
	for i := 0; i < arrayLength; i++ {
		element, err := decode(reader)
		if err != nil {
			ctx.Logger.Printf("Error trying to decode array element in decodeArray: %s", err)
			return constants.DataRepr{}, err
		}
		array = append(array, element)
	}

	return constants.DataRepr{
		Type:  constants.ARRAY,
		Data:  nil,
		Array: array,
	}, nil
}
