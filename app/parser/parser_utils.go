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
