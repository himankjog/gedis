package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
)

func TestEncode_String(t *testing.T) {
	dataString := "This is a string"
	data := constants.DataRepr{
		Type: constants.STRING,
		Data: []byte(dataString),
	}
	expectedOutput := string(constants.STRING) + dataString + constants.CRLF

	encodedResponse := Encode(data)
	if !bytes.Equal([]byte(expectedOutput), encodedResponse) {
		t.Errorf("Expected encoded string: %s, Got: %s", expectedOutput, string(encodedResponse))
	}
}

func TestEncode_BulkString_WithData(t *testing.T) {
	dataString := "Bulk String Content"
	data := constants.DataRepr{
		Type: constants.BULK,
		Data: []byte(dataString),
	}
	expectedOutput := string(constants.BULK) + strconv.Itoa(len(dataString)) + constants.CRLF + dataString + constants.CRLF

	encodedResponse := Encode(data)
	if !bytes.Equal([]byte(expectedOutput), encodedResponse) {
		t.Errorf("Expected encoded bulk string: %s, Got: %s", expectedOutput, string(encodedResponse))
	}
}

func TestEncode_BulkString_WithNilData(t *testing.T) {
	data := constants.DataRepr{
		Type: constants.BULK,
		Data: nil,
	}
	expectedOutput := string(constants.BULK) + "-1" + constants.CRLF

	encodedResponse := Encode(data)
	if !bytes.Equal([]byte(expectedOutput), encodedResponse) {
		t.Errorf("Expected encoded null bulk string: %s, Got: %s", expectedOutput, string(encodedResponse))
	}
}

func TestEncode_Integer(t *testing.T) {
	data := constants.DataRepr{
		Type: constants.INTEGER,
		Data: []byte("123"),
	}
	expectedOutput := string(constants.INTEGER) + "123" + constants.CRLF

	encodedResponse := Encode(data)
	if !bytes.Equal([]byte(expectedOutput), encodedResponse) {
		t.Errorf("Expected encoded integer: %s, Got: %s", expectedOutput, string(encodedResponse))
	}
}

func TestEncode_Error(t *testing.T) {
	data := constants.DataRepr{
		Type: constants.ERROR,
		Data: []byte("An error message"),
	}
	expectedPrefix := string(constants.ERROR) + "An error message" + constants.CRLF

	encodedResponse := Encode(data)
	if !bytes.HasPrefix(encodedResponse, []byte(expectedPrefix)) {
		t.Errorf("Expected encoded error to start with: %s, Got: %s", expectedPrefix, string(encodedResponse))
	}
}

func TestEncode_Array(t *testing.T) {
	dataArray := []constants.DataRepr{
		{Type: constants.STRING, Data: []byte("String 1")},
		{Type: constants.INTEGER, Data: []byte("42")},
	}
	expectedOutput := string(constants.ARRAY) + "2" + constants.CRLF
	expectedOutput += string(constants.STRING) + "String 1" + constants.CRLF
	expectedOutput += string(constants.INTEGER) + "42" + constants.CRLF

	encodedResponse := Encode(constants.DataRepr{Type: constants.ARRAY, Array: dataArray})
	if !bytes.Equal([]byte(expectedOutput), encodedResponse) {
		t.Errorf("Expected encoded array: %s, Got: %s", expectedOutput, string(encodedResponse))
	}
}

func TestEncode_UnsupportedType(t *testing.T) {
	unsupportedType := constants.DataRepr{Type: 'X', Data: []byte("Some data")} // Provide some data to avoid nil pointer dereference
	expectedErrorMessage := fmt.Sprintf("Unsupported data type: %q", unsupportedType.Type)
	expectedOutput := "-" + expectedErrorMessage + constants.CRLF

	encodedResponse := Encode(unsupportedType)
	if !bytes.Equal([]byte(expectedOutput), encodedResponse) {
		t.Errorf("Expected encoded error for unsupported type: %q, Got: %q", expectedOutput, string(encodedResponse))
	}
}
