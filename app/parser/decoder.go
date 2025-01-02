package parser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type decoder func(reader *bufio.Reader) (*constants.DataRepr, error)

var decoderRegistry map[byte]decoder

func init() {
	decoderRegistry = make(map[byte]decoder)
	decoderRegistry[constants.INTEGER] = decodeInteger
	decoderRegistry[constants.STRING] = decodeSimpleString
	decoderRegistry[constants.BULK] = decodeBulkString
	decoderRegistry[constants.ERROR] = decodeError
	decoderRegistry[constants.ARRAY] = decodeArray
}

func Decode(data []byte) ([]constants.DataRepr, error) {
	if len(data) == 0 {
		return nil, errors.New("no data provided")
	}
	reader := bufio.NewReader(bytes.NewReader(data))
	decodedDataList := make([]constants.DataRepr, 0)
	for {
		decodedData, err := decode(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		decodedDataList = append(decodedDataList, *decodedData)

		if reader.Buffered() == 0 {
			break
		}
	}
	return decodedDataList, nil
}

func decode(reader *bufio.Reader) (*constants.DataRepr, error) {
	dataTypeByte, err := reader.Peek(1)
	if err != nil {
		ctx.Logger.Printf("Error trying to start decode: %s", err)
		return nil, err
	}

	decoder, decoderExists := decoderRegistry[dataTypeByte[0]]
	if !decoderExists {
		errMessage := fmt.Sprintf("%q is not a valid start of a RESP2 value (expected +, -, :, $ or *) for decoder", dataTypeByte[0])
		ctx.Logger.Println(errMessage)
		return nil, errors.New(errMessage)
	}
	return decoder(reader)
}

func decodeInteger(reader *bufio.Reader) (*constants.DataRepr, error) {
	data, _, err := ReadUntilCRLF(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read integer in decodeInteger: %s", err)
		return nil, err
	}
	integer, err := strconv.Atoi(string(data[1:]))
	if err != nil {
		ctx.Logger.Printf("Error trying to read integer data in decodeInteger: %s", err)
		return nil, err
	}
	ctx.Logger.Printf("Parsed integer: %d", integer)
	integerDataRepr := utils.CreateIntegerResponse(string(data[1:]))
	return &integerDataRepr, nil
}

func decodeSimpleString(reader *bufio.Reader) (*constants.DataRepr, error) {
	simpleString, _, err := ReadUntilCRLF(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read simple string in decodeSimpleString: %s", err)
		return nil, err
	}
	ctx.Logger.Printf("Parsed simple string: %s", simpleString)
	stringDataRepr := utils.CreateStringResponse(string(simpleString[1:]))
	return &stringDataRepr, nil
}

func decodeBulkString(reader *bufio.Reader) (*constants.DataRepr, error) {
	bulkStringLengthBytes, _, err := ReadUntilCRLF(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read bulk string length bytes in decodeBulkString: %s", err)
		return nil, err
	}
	bulkStringLength, err := strconv.Atoi(string(bulkStringLengthBytes[1:]))
	if err != nil {
		ctx.Logger.Printf("Error trying to parse bulk string length from bulk string length bytes: %s", err)
		return nil, err
	}
	if bulkStringLength == -1 {
		nilBulkStringResponse := utils.NilBulkStringResponse()
		return &nilBulkStringResponse, nil
	}
	bulkStringBytes := make([]byte, bulkStringLength)
	_, err = reader.Read(bulkStringBytes)
	if err != nil {
		ctx.Logger.Printf("Error trying to read bulk string bytes in decodeBulkString: %s", err)
		return nil, err
	}

	crlfData, _ := reader.Peek(2)

	if len(crlfData) == 2 && crlfData[0] == '\r' {
		// Discard \r\n
		reader.Discard(2)
	}
	bulkStringDataRepr := utils.CreateBulkResponse(string(bulkStringBytes))
	return &bulkStringDataRepr, nil
}

func decodeError(reader *bufio.Reader) (*constants.DataRepr, error) {
	errorData, _, err := ReadUntilCRLF(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read error data in decodeError: %s", err)
		return nil, err
	}
	ctx.Logger.Printf("Parsed error: %s", errorData)
	errorDataRepr := utils.CreateErrorResponse(string(errorData[1:]))
	return &errorDataRepr, nil
}

func decodeArray(reader *bufio.Reader) (*constants.DataRepr, error) {
	arrayLengthBytes, _, err := ReadUntilCRLF(reader)
	if err != nil {
		ctx.Logger.Printf("Error trying to read array length bytes in decodeArray: %s", err)
		return nil, err
	}

	arrayLength, err := strconv.Atoi(string(arrayLengthBytes[1:]))
	if err != nil {
		ctx.Logger.Printf("Error trying to parse array length from array length bytes: %s", err)
		return nil, err
	}

	var array []constants.DataRepr
	for i := 0; i < arrayLength; i++ {
		element, err := decode(reader)
		if err != nil {
			ctx.Logger.Printf("Error trying to decode array element in decodeArray: %s", err)
			return nil, err
		}
		array = append(array, *element)
	}
	arrayDataRepr := utils.CreateArrayDataRepr(array)
	return &arrayDataRepr, nil
}
