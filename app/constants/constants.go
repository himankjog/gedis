package constants

type Type byte

const (
	INTEGER = ':'
	STRING  = '+'
	BULK    = '$'
	ARRAY   = '*'
	ERROR   = '-'
)

type DataRepr struct {
	Type  Type
	Data  []byte
	Array []DataRepr
}

const (
	CRLF = "\r\n"
)
