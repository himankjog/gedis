package parser

import (
	"bytes"
	"testing"
)

func validResponse(expectedResponse RESP, actualResponse RESP) bool {
	if actualResponse.Type != expectedResponse.Type {
		return false
	}
	if !bytes.Equal(actualResponse.Data, expectedResponse.Data) {
		return false
	}
	if len(expectedResponse.Array) != len(actualResponse.Array) {
		return false
	}
	arrayValidationStatus := true
	for i, element := range expectedResponse.Array {
		arrayValidationStatus = arrayValidationStatus && validResponse(element, actualResponse.Array[i])
	}
	return arrayValidationStatus
}

func TestDecodeInteger_ValidInteger(t *testing.T) {
	input := []byte(":123\r\n")
	expected := RESP{
		Type:  INTEGER,
		Data:  []byte("123"),
		Array: nil,
	}

	expectValidResponse(t, input, expected)
}

func TestDecodeInteger_ValidPositiveInteger(t *testing.T) {
	input := []byte(":+123\r\n")
	expected := RESP{
		Type:  INTEGER,
		Data:  []byte("+123"),
		Array: nil,
	}

	expectValidResponse(t, input, expected)
}

func TestDecodeInteger_NegativeInteger(t *testing.T) {
	input := []byte(":-123\r\n")
	expected := RESP{
		Type:  INTEGER,
		Data:  []byte("-123"),
		Array: nil,
	}

	expectValidResponse(t, input, expected)
}

func TestDecodeInteger_InvalidInteger_NoPrefixColon(t *testing.T) {
	input := []byte("123\r\n")
	expectedErr := "unsupported data type"
	expectError(t, input, expectedErr)
}

func TestDecodeInteger_InvalidInteger_UnsupportedPrefix(t *testing.T) {
	input := []byte("^123\r\n")
	expectedErr := "unsupported data type"
	expectError(t, input, expectedErr)
}

func TestDecodeInteger_InvalidInteger_MissingNextLineSuffix(t *testing.T) {
	input := []byte(":123\r")
	expectedErr := "EOF"
	expectError(t, input, expectedErr)
}

func TestSimpleString_ValidSimpleString(t *testing.T) {
	input := []byte("+ABC DEF\r\n")
	expected := RESP{
		Type:  STRING,
		Data:  []byte("ABC DEF"),
		Array: nil,
	}

	expectValidResponse(t, input, expected)
}

func TestSimpleString_InvalidSimpleString_NoPrefixColon(t *testing.T) {
	input := []byte("ABC\r\n")
	expectedErr := "unsupported data type"
	expectError(t, input, expectedErr)
}

func TestSimpleString_InvalidSimpleString_UnsupportedPrefix(t *testing.T) {
	input := []byte("^ABC\r\n")
	expectedErr := "unsupported data type"
	expectError(t, input, expectedErr)
}

func TestSimpleString_InvalidSimpleString_MissingNextLineSuffix(t *testing.T) {
	input := []byte("+ABC\r")
	expectedErr := "EOF"
	expectError(t, input, expectedErr)
}

func TestBulkString_ValidBulkString(t *testing.T) {
	input := []byte("$5\r\nhello\r\n")
	expected := RESP{
		Type:  BULK,
		Data:  []byte("hello"),
		Array: nil,
	}

	expectValidResponse(t, input, expected)
}

func TestBulkString_EmptyBulkString(t *testing.T) {
	input := []byte("$0\r\n\r\n")
	expected := RESP{
		Type:  BULK,
		Data:  []byte(""),
		Array: nil,
	}

	expectValidResponse(t, input, expected)
}

func TestBulkString_InvalidBulkString_InvalidLength(t *testing.T) {
	input := []byte("$a\r\nhello\r\n")
	expectedErr := "strconv.Atoi: parsing \"a\": invalid syntax"
	expectError(t, input, expectedErr)
}

func TestBulkString_InvalidBulkStringLength_MissingNextLineSuffix(t *testing.T) {
	input := []byte("$5\rhello\r\n")
	expectedErr := "EOF"
	expectError(t, input, expectedErr)
}

func TestBulkString_InvalidBulkStringLength_MoreThanDefinedData(t *testing.T) {
	input := []byte("$5\r\nhelloWorld\r\n")
	expectedErr := "bulk string does not end with CRLF"
	expectError(t, input, expectedErr)
}

func TestBulkString_InvalidBulkStringLength_LessThanDefinedData(t *testing.T) {
	input := []byte("$5\r\nhel\r\n")
	expectedErr := "provided length does not match provided data length"
	expectError(t, input, expectedErr)
}

func TestBulkString_ValidOneDimensionalArray(t *testing.T) {
	input := []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
	expected := RESP{
		Type: ARRAY,
		Data: nil,
		Array: []RESP{
			{
				Type:  BULK,
				Data:  []byte("hello"),
				Array: nil,
			},
			{
				Type:  BULK,
				Data:  []byte("world"),
				Array: nil,
			},
		},
	}

	expectValidResponse(t, input, expected)
}

func TestBulkString_ValidNestedArray(t *testing.T) {
	input := []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n")
	expected := RESP{
		Type: ARRAY,
		Data: nil,
		Array: []RESP{
			{
				Type: ARRAY,
				Data: nil,
				Array: []RESP{
					{
						Type:  INTEGER,
						Data:  []byte("1"),
						Array: nil,
					},
					{
						Type:  INTEGER,
						Data:  []byte("2"),
						Array: nil,
					},
					{
						Type:  INTEGER,
						Data:  []byte("3"),
						Array: nil,
					},
				},
			},
			{
				Type: ARRAY,
				Data: nil,
				Array: []RESP{
					{
						Type:  STRING,
						Data:  []byte("Hello"),
						Array: nil,
					},
					{
						Type:  ERROR,
						Data:  []byte("World"),
						Array: nil,
					},
				},
			},
		},
	}
	expectValidResponse(t, input, expected)
}

func TestBulkString_InvalidOneDimensionalArray(t *testing.T) {
	input := []byte("*2\r$5\r\nhello\r\n$5\r\nworld\r\n")
	expectedErr := "strconv.Atoi: parsing \"2\\r$5\": invalid syntax"
	expectError(t, input, expectedErr)
}

func expectValidResponse(t *testing.T, input []byte, expected RESP) {
	resp, err := Decode(input)
	if err != nil {
		t.Errorf("decodeInteger(%q) returned error %v", input, err)
	}
	if !validResponse(expected, resp) {
		t.Errorf("decodeInteger(%q) = %v, want %v", input, resp, expected)
	}
}

func expectError(t *testing.T, input []byte, expectedError string) {
	expected := RESP{}

	resp, err := Decode(input)
	if err == nil {
		t.Errorf("Decode(%q) = %v, want error", input, resp)
	}
	if err.Error() != expectedError {
		t.Errorf("Decode(%q) returned error %v, want %v", input, err, expectedError)
	}
	if !validResponse(expected, resp) {
		t.Errorf("Decode(%q) = %v, want %v", input, resp, expected)
	}
}
