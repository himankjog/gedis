package parser

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"strconv"
)

func Decode(data []byte) (RESP, error) {
	if len(data) == 0 {
		return RESP{}, errors.New("no data provided")
	}
	reader := bufio.NewReader(bytes.NewReader(data))
	return decode(reader)
}

func decode(reader *bufio.Reader) (RESP, error) {
	data, _, err := ReadNext(reader)
	if err != nil {
		log.Printf("Error trying to start decode: %s", err)
		return RESP{}, err
	}
	switch data[0] {
	case INTEGER:
		return decodeInteger(data[1:])
	case STRING:
		return decodeSimpleString(data[1:])
	case BULK:
		bulkStringData, _, err := ReadNext(reader)
		if err != nil {
			log.Printf("Error trying to read bulk string data from decode: %s", err)
			return RESP{}, err
		}
		bulkStringData = append(data, bulkStringData...)
		return decodeBulkString(bulkStringData[1:])
	case ERROR:
		return decodeError(data[1:])
	case ARRAY:
		return decodeArray(data[1:], reader)
	default:
		log.Printf("Unsupported data type: %v", data[0])
		return RESP{}, errors.New("unsupported data type")
	}
}

func decodeInteger(data []byte) (RESP, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	integerData, charactersRead, err := ReadNext(reader)
	if err != nil {
		log.Printf("Error trying to read integer data in decodeInteger: %s", err)
		return RESP{}, errors.New("error parsing integer data")
	}
	integer, err := strconv.Atoi(string(integerData[:charactersRead]))
	if err != nil {
		log.Printf("Error trying to parse integer from integer data: %s", err)
		return RESP{}, errors.New("error parsing integer from integer data")
	}
	log.Printf("Parsed integer: %d", integer)
	return RESP{
		Type:  INTEGER,
		Data:  integerData[:charactersRead],
		Array: nil,
	}, nil
}

func decodeSimpleString(data []byte) (RESP, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	simpleString, charactersRead, err := ReadNext(reader)
	if err != nil {
		log.Printf("Error trying to read simple string in decodeSimpleString: %s", err)
		return RESP{}, err
	}
	log.Printf("Parsed simple string: %s", simpleString)
	return RESP{
		Type:  STRING,
		Data:  simpleString[:charactersRead],
		Array: nil,
	}, nil
}

func decodeBulkString(data []byte) (RESP, error) {
	bulkStringReader := bufio.NewReader(bytes.NewReader(data))
	bulkStringLengthBytes, charactersRead, err := ReadNext(bulkStringReader)
	if err != nil {
		log.Printf("Error trying to read bulk string length bytes in decodeBulkString: %s", err)
		return RESP{}, err
	}
	bulkStringLength, err := strconv.Atoi(string(bulkStringLengthBytes[:charactersRead]))
	if err != nil {
		log.Printf("Error trying to parse bulk string length from bulk string length bytes: %s", err)
		return RESP{}, err
	}
	bulkStringBytes := make([]byte, bulkStringLength+2)
	readStringLength, err := bulkStringReader.Read(bulkStringBytes)
	if err != nil {
		log.Printf("Error trying to read bulk string bytes in decodeBulkString: %s", err)
		return RESP{}, err
	}
	if readStringLength != bulkStringLength+2 {
		log.Printf("Error: read string length does not match bulk string length")
		return RESP{}, errors.New("provided length does not match provided data length")
	}

	if readStringLength > 2 {
		lastChar := bulkStringBytes[readStringLength-1]
		secondToLastChar := bulkStringBytes[readStringLength-2]
		if secondToLastChar != '\r' || lastChar != '\n' {
			log.Printf("Error: bulk string does not end with CRLF")
			return RESP{}, errors.New("bulk string does not end with CRLF")
		}
	}

	return RESP{
		Type:  BULK,
		Data:  bulkStringBytes[:bulkStringLength],
		Array: nil,
	}, nil
}

func decodeError(data []byte) (RESP, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	errorData, charactersRead, err := ReadNext(reader)
	if err != nil {
		log.Printf("Error trying to read error data in decodeError: %s", err)
		return RESP{}, err
	}
	log.Printf("Parsed error: %s", errorData)
	return RESP{
		Type:  ERROR,
		Data:  errorData[:charactersRead],
		Array: nil,
	}, nil
}

func decodeArray(data []byte, reader *bufio.Reader) (RESP, error) {
	arrayReader := bufio.NewReader(bytes.NewReader(data))
	arrayLengthBytes, charactersRead, err := ReadNext(arrayReader)
	if err != nil {
		log.Printf("Error trying to read array length bytes in decodeArray: %s", err)
		return RESP{}, err
	}

	arrayLength, err := strconv.Atoi(string(arrayLengthBytes[:charactersRead]))
	if err != nil {
		log.Printf("Error trying to parse array length from array length bytes: %s", err)
		return RESP{}, err
	}

	var array []RESP
	for i := 0; i < arrayLength; i++ {
		element, err := decode(reader)
		if err != nil {
			log.Printf("Error trying to decode array element in decodeArray: %s", err)
			return RESP{}, err
		}
		array = append(array, element)
	}

	return RESP{
		Type:  ARRAY,
		Data:  nil,
		Array: array,
	}, nil
}
