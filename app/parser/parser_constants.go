package parser

type Type byte

const (
	INTEGER = ':'
	STRING  = '+'
	BULK    = '$'
	ARRAY   = '*'
	ERROR   = '-'
)

type RESP struct {
	Type  Type
	Data  []byte
	Array []RESP
}
