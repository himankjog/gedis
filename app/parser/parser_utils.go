package parser

import (
	"bufio"
	"errors"
)

func ReadNext(reader *bufio.Reader) ([]byte, int, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, 0, errors.New("carriage return character not found")
	}
	charactersRead := len(line) - 2
	return line, charactersRead, nil
}

func ReadUntilCRLF(reader *bufio.Reader) ([]byte, int, error) {
	data, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, 0, err
	}

	dataLength := len(data)

	if dataLength < 2 || data[dataLength-2] != '\r' {
		return nil, 0, errors.New("Invalid data: missing CRLF")
	}

	return data[:dataLength-2], dataLength - 2, nil
}
