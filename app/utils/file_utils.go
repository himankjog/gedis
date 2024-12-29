package utils

import (
	"encoding/hex"
	"os"
)

func ReadHexFileToBinary(filepath string) ([]byte, error) {
	hexData, err := os.ReadFile(filepath)
	if err != nil {
		ctx.Logger.Printf("Error while trying to read file at path '%s': %v", filepath, err.Error())
		return nil, err
	}

	decodedHexData := make([]byte, hex.DecodedLen(len(hexData)))
	decodedByteLen, err := hex.Decode(decodedHexData, hexData)

	if err != nil {
		ctx.Logger.Printf("Error while trying to decode hex data from file at path '%s': %v", filepath, err.Error())
		return nil, err
	}
	ctx.Logger.Printf("Successfully decoded %d hex data bytes from file '%s'", decodedByteLen, filepath)

	return decodedHexData, nil
}
