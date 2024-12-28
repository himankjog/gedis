package handlers

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	bimap "github.com/codecrafters-io/redis-starter-go/app/utils/bimap"
	requestUtils "github.com/codecrafters-io/redis-starter-go/app/utils/request"
	"github.com/google/uuid"
)

type HandshakeStep struct {
	CommandName      string
	Request          constants.DataRepr
	ExpectedResponse constants.DataRepr
}

var CONN_FILE_DESC_BI_MAP = bimap.NewBiMap[int, net.Conn]()
var EPOLL_FD = -1

func StartEventLoop(serverInstance *server.Server) {
	var err error
	EPOLL_FD, err = syscall.EpollCreate1(0)

	if err != nil {
		log.Fatalf("Error creating epoll: %v", err.Error())
	}
	go handleMasterConnection(serverInstance)
	go acceptConnections((*serverInstance).Listener)

	events := make([]syscall.EpollEvent, 100)

	for {
		n, err := syscall.EpollWait(EPOLL_FD, events, -1)
		if err != nil {
			log.Fatalf("Error waiting for epoll events: %v", err.Error())
		}
		log.Printf("Epoll has %d events", n)
		for i := 0; i < n; i++ {
			if events[i].Fd == -1 {
				continue
			}
			connFd := int(events[i].Fd)
			conn, isConnPresent := CONN_FILE_DESC_BI_MAP.Lookup(connFd)
			if !isConnPresent {
				log.Printf("Connection object not found for connection file descriptor: %d. Terminating the connection", connFd)
				terminateConnection(conn)
				continue
			}
			dataFromConn, err := fetchDataFromConnection(conn)
			if err != nil {
				terminateConnection(conn)
				continue
			}
			if len(dataFromConn) == 0 {
				continue
			}
			requestId := uuid.New()
			log.Printf("[%s] Received request ID '%s' with data: %q", conn.RemoteAddr(), requestId.String(), dataFromConn)
			response := ProcessRequest(Request{
				Data:      dataFromConn,
				RequestId: requestId,
			})
			go writeDataToConnection(conn, response)
		}
	}
}

func acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue
		}

		connFd := getConnectionFileDescriptor(conn)
		log.Printf("[%s] Got connection file descriptor: %d", conn.RemoteAddr(), connFd)

		CONN_FILE_DESC_BI_MAP.Insert(connFd, conn)
		err = syscall.EpollCtl(EPOLL_FD, syscall.EPOLL_CTL_ADD, connFd, &syscall.EpollEvent{
			Events: syscall.EPOLLIN,
			Fd:     int32(connFd),
		})
		if err != nil {
			log.Printf("[%s] Error adding connection to epoll: %v", conn.RemoteAddr(), err.Error())
			terminateConnection(conn)
			continue
		}

		log.Printf("[%s] Connection established", conn.RemoteAddr())
	}
}

func fetchDataFromConnection(conn net.Conn) ([]byte, error) {
	log.Printf("[%s] Started reading data from connection", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	data := make([]byte, 0, 1024)
	for {
		readBuf := make([]byte, 1024)
		bytesRead, err := reader.Read(readBuf)
		log.Printf("[%s] Read %d bytes from connection", conn.RemoteAddr(), bytesRead)
		if err != nil {
			fmt.Errorf("[%s] Error reading data from: %v", conn.RemoteAddr(), err.Error())
			if err != io.EOF {
				return nil, err
			}
			break
		}
		data = append(data, readBuf[:bytesRead]...)
		if bytesRead < 1024 {
			// If we don't have anymore data to read, we can safely quit the read loop
			break
		}
	}
	if len(data) < 1 {
		log.Printf("[%s] No data received from connection", conn.RemoteAddr())
	}

	return data, nil
}

func handleMasterConnection(serverInstance *server.Server) {
	serverRole := (*serverInstance).ReplicationConfig.Role
	if serverRole == constants.MASTER_ROLE {
		return
	}
	serverAddress := (*serverInstance).ServerAddress
	masterAddress := (*serverInstance).ReplicationConfig.MasterServerAddress
	_, err := initiateHandShakeWithMaster(serverInstance)
	if err != nil {
		log.Fatalf("[%s] Error while trying to initiate handshake with master (%s): %v", serverAddress, masterAddress, err.Error())
	}

	log.Printf("[%s] Handshake completed with master at address: %s", serverAddress, masterAddress)
}

func initiateHandShakeWithMaster(serverInstance *server.Server) (net.Conn, error) {
	masterServerAddress := (*serverInstance).ReplicationConfig.MasterServerAddress
	hostServerAddress := (*serverInstance).ServerAddress
	conn, err := net.Dial("tcp", masterServerAddress)

	if err != nil {
		fmt.Errorf("[%s] Error while trying to establish connection with master (%s): %v", hostServerAddress, masterServerAddress, err.Error())
		return nil, err
	}
	handshakePipeline := createHandshakePipeline(serverInstance)
	for _, handshakeStep := range handshakePipeline {
		cmd := handshakeStep.CommandName
		log.Printf("[%s] Beginning the handshake step with master (%s) using command [%s]", hostServerAddress, masterServerAddress, cmd)

		err := writeDataToConnection(conn, handshakeStep.Request)
		if err != nil {
			fmt.Errorf("[%s] Error while trying to send command [%s] master (%s): %v", hostServerAddress, cmd, masterServerAddress, err.Error())
			return nil, err
		}

		responseFromMaster, err := fetchDataFromConnection(conn)
		if err != nil {
			fmt.Errorf("[%s] Error while waiting for response from master (%s) for command [%s]: %v", hostServerAddress, masterServerAddress, cmd, err.Error())
			return nil, err
		}
		if len(responseFromMaster) == 0 {
			errMessage := fmt.Sprintf("[%s] Empty response received from master (%s) for command [%s]: %v", hostServerAddress, masterServerAddress, cmd, err.Error())
			fmt.Errorf(errMessage)
			return nil, errors.New(errMessage)
		}
		log.Printf("[%s] Received response from master (%s) for command [%s]: %q", hostServerAddress, masterServerAddress, cmd, responseFromMaster)

		decodedResponseFromMaster, err := parser.Decode(responseFromMaster)
		if err != nil {
			fmt.Errorf("[%s] Error while decoding for response from master (%s) for command [%s]: %v", hostServerAddress, masterServerAddress, cmd, err.Error())
			return nil, err
		}
		if cmd == constants.PSYNC_COMMAND {
			// TODO: Validate PSYNC response
			continue
		}
		if !handshakeStep.ExpectedResponse.IsEqual(decodedResponseFromMaster) {
			fmt.Errorf("[%s] Unexpected response from master (%s) for command [%s]: %+v", hostServerAddress, masterServerAddress, cmd, decodedResponseFromMaster)
			return nil, err
		}
	}
	log.Printf("[%s] Partial handshake completed with master (%s)", hostServerAddress, masterServerAddress)
	return conn, nil
}

func createHandshakePipeline(serverInstance *server.Server) []HandshakeStep {
	handshakePipeline := []HandshakeStep{}
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.PING_COMMAND,
		Request:          requestUtils.CreateRequestForCommand(constants.PING_COMMAND),
		ExpectedResponse: requestUtils.CreateStringResponse(constants.PONG_RESPONSE),
	})
	ok_response := requestUtils.CreateStringResponse(constants.OK_RESPONSE)
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.REPLCONF_COMMAND,
		Request:          requestUtils.CreateRequestForCommand(constants.REPLCONF_COMMAND, constants.REPLCONF_LISTENING_PORT_PARAM, (serverInstance).ListeningPort),
		ExpectedResponse: ok_response,
	})
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.REPLCONF_COMMAND,
		Request:          requestUtils.CreateRequestForCommand(constants.REPLCONF_COMMAND, constants.REPLCONF_CAPA_PARAM, constants.REPLCONF_PSYNC2_PARAM),
		ExpectedResponse: ok_response,
	})
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.PSYNC_COMMAND,
		Request:          requestUtils.CreateRequestForCommand(constants.PSYNC_COMMAND, constants.PSYNC_UNKNOWN_REPLICATION_ID_PARAM, constants.PSYNC_UNKNOWN_MASTER_OFFSET),
		ExpectedResponse: ok_response,
	})

	return handshakePipeline
}

func getConnectionFileDescriptor(conn net.Conn) int {
	tcpConn := conn.(*net.TCPConn)
	file, err := tcpConn.File()

	if err != nil {
		log.Fatalf("Error getting file descriptor: %v", err.Error())
	}
	return int(file.Fd())
}

func terminateConnection(conn net.Conn) {
	connFd, connFdPresent := CONN_FILE_DESC_BI_MAP.ReverseLookup(conn)
	if !connFdPresent {
		log.Printf("[%s] Connection file descriptor not found in connection file descriptor map while terminating connection", conn.RemoteAddr())
		os.Exit(1)
	}

	closeConnectionPoll(connFd)
	closeConnection(conn)
}

func closeConnectionPoll(connFd int) {
	err := syscall.EpollCtl(EPOLL_FD, syscall.EPOLL_CTL_DEL, connFd, nil)
	if err != nil {
		log.Printf("Failed to remove fd %d from epoll: %v", connFd, err)
	}
	CONN_FILE_DESC_BI_MAP.Delete(connFd)
	log.Printf("[ConnFD-%d] Connection closed", connFd)
}

func closeConnection(conn net.Conn) {
	err := conn.Close()
	if err != nil {
		log.Printf("[%s] Error closing connection: '%v'", conn.RemoteAddr(), err.Error())
		os.Exit(1)
	}
	CONN_FILE_DESC_BI_MAP.DeleteUsingReverseLookup(conn)
	log.Printf("[%s] Connection closed", conn.RemoteAddr())
}

func writeDataToConnection(conn net.Conn, data constants.DataRepr) error {
	encodedData := parser.Encode(data)
	log.Printf("[%s] Begin writing data '%q' to connection", conn.RemoteAddr(), encodedData)
	_, err := conn.Write(encodedData)
	if err != nil {
		fmt.Errorf("[%s] Error writing data '%q' to connection: %v", conn.RemoteAddr(), encodedData, err.Error())
		return err
	}
	log.Printf("[%s] Successfully written data '%q' to connection", conn.RemoteAddr(), encodedData)
	return nil
}
