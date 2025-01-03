package constants

import (
	"bytes"
	"strings"

	"github.com/google/uuid"
)

// Server constants
const (
	DEFAULT_SERVER_ADDRESS = "0.0.0.0"
	DEFAULT_SERVER_PORT    = "6379"
	MASTER_ROLE            = "master"
	REPLICA_ROLE           = "slave"
)

// Common Structs
type Request struct {
	Data      []byte
	RequestId uuid.UUID
}

type ExecuteCommandRequest struct {
	Cmd            string
	RequestId      uuid.UUID
	Args           []DataRepr
	DecodedRequest DataRepr
}

// Parser constants
type DataType byte

const (
	INTEGER  = ':'
	STRING   = '+'
	BULK     = '$'
	ARRAY    = '*'
	ERROR    = '-'
	RDB_FILE = 'f'
)

type DataRepr struct {
	Type  DataType
	Data  []byte
	Array []DataRepr
}

const (
	CRLF = "\r\n"
)

// Command Constants
const (
	PING_COMMAND     = "PING"
	ECHO_COMMAND     = "ECHO"
	GET_COMMAND      = "GET"
	SET_COMMAND      = "SET"
	INFO_COMMAND     = "INFO"
	REPLCONF_COMMAND = "REPLCONF"
	PSYNC_COMMAND    = "PSYNC"
)

const (
	PONG_RESPONSE       = "PONG"
	OK_RESPONSE         = "OK"
	FULLRESYNC_RESPONSE = "FULLRESYNC"
)

const (
	SUB_COMMAND_FORMAT = "%s_%s"
)

// Sub-commands
const (
	GETACK = "GETACK"
	ACK    = "ACK"
)

const (
	SET_PX_COMMAND = "SET_PX"
	// REPLCONF
	REPLCONF_LISTENING_PORT_PARAM = "listening-port"
	REPLCONF_CAPA_PARAM           = "capa"
	REPLCONF_PSYNC2_PARAM         = "psync2"
	REPLCONF_GETACK               = "REPLCONF_GETACK"
	REPLCONF_ACK                  = "REPLCONF_ACK"
	// PSYNC
	PSYNC_UNKNOWN_REPLICATION_ID_PARAM = "?"
	PSYNC_UNKNOWN_MASTER_OFFSET        = "-1"
)

type CommandExecutedNotification struct {
	Cmd            string
	RequestId      uuid.UUID
	Args           []DataRepr
	DecodedRequest DataRepr
	Success        bool
}

func (actual DataRepr) IsEqual(expected DataRepr, onlyPrefix bool) bool {
	if actual.Type != expected.Type {
		return false
	}

	switch actual.Type {
	case INTEGER, STRING, BULK, ERROR:
		return (bytes.Equal(actual.Data, expected.Data) ||
			(onlyPrefix && strings.HasPrefix(string(actual.Data), string(expected.Data))))
	case ARRAY:
		if len(actual.Array) != len(expected.Array) {
			return false
		}
		for i := range actual.Array {
			if !actual.Array[i].IsEqual(expected.Array[i], onlyPrefix) {
				return false
			}
		}
		return true

	default:
		return false
	}
}
