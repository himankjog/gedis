package constants

import "bytes"

// Server constants
const (
	DEFAULT_SERVER_ADDRESS = "0.0.0.0"
	DEFAULT_SERVER_PORT    = "6379"
	MASTER_ROLE            = "master"
	REPLICA_ROLE           = "slave"
)

// Parser constants
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

// Command Constants
const (
	PING_COMMAND = "PING"
	ECHO_COMMAND = "ECHO"
	GET_COMMAND  = "GET"
	SET_COMMAND  = "SET"
	INFO_COMMAND = "INFO"

	REPLCONF_COMMAND = "REPLCONF"
)

const (
	PONG_RESPONSE = "PONG"
	OK_RESPONSE   = "OK"
)

const (
	SUB_COMMAND_FORMAT = "%s_%s"
)

// Sub-commands
const (
	SET_PX_COMMAND                = "SET_PX"
	REPLCONF_LISTENING_PORT_PARAM = "listening-port"
	REPLCONF_CAPA_PARAM           = "capa"
	REPLCONF_PSYNC2_PARAM         = "psync2"
)

func (d1 DataRepr) IsEqual(d2 DataRepr) bool {
	if d1.Type != d2.Type {
		return false
	}

	switch d1.Type {
	case INTEGER, STRING, BULK, ERROR:
		return bytes.Equal(d1.Data, d2.Data)
	case ARRAY:
		if len(d1.Array) != len(d2.Array) {
			return false
		}
		for i := range d1.Array {
			if !d1.Array[i].IsEqual(d2.Array[i]) {
				return false
			}
		}
		return true

	default:
		return false
	}
}
