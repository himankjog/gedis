package handlers

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"github.com/google/uuid"
)

type GedisConn struct {
	conn net.Conn
	ctx  *context.Context
}

type HandshakeStep struct {
	CommandName      string
	Request          constants.DataRepr
	ExpectedResponse constants.DataRepr
}

type ConnectionHandler struct {
	connectionFileDescriptorBiMap *utils.BiMap[int, net.Conn]
	requestIdToConnMap            map[uuid.UUID]net.Conn
	epollFd                       int
	ctx                           *context.Context
	requestHandler                *RequestHandler
}

func InitConnectionHandler(ctx *context.Context, requestHandler *RequestHandler) *ConnectionHandler {
	connectionHandler := ConnectionHandler{
		connectionFileDescriptorBiMap: utils.NewBiMap[int, net.Conn](),
		epollFd:                       -1,
		ctx:                           ctx,
		requestHandler:                requestHandler,
		requestIdToConnMap:            make(map[uuid.UUID]net.Conn),
	}
	return &connectionHandler
}

func (h *ConnectionHandler) StartEventLoop() {
	epollFd, err := syscall.EpollCreate1(0)
	if err != nil {
		h.ctx.Logger.Fatalf("Error creating epoll: %v", err.Error())
	}
	h.epollFd = epollFd
	go h.handleMasterConnection()
	go h.acceptConnections()

	events := make([]syscall.EpollEvent, 100)

	for {
		n, err := syscall.EpollWait(h.epollFd, events, -1)
		if err != nil {
			h.ctx.Logger.Fatalf("Error waiting for epoll events: %v", err.Error())
		}
		// h.ctx.Logger.Printf("Epoll has %d events", n)
		for i := 0; i < n; i++ {
			if events[i].Fd == -1 {
				continue
			}
			go h.processEventForConnection(int(events[i].Fd))
		}
	}
}

func (h *ConnectionHandler) GetConnectionForRequest(requestId uuid.UUID) (*net.Conn, error) {
	h.ctx.Logger.Printf("Fetching connection for requestId: %s", requestId.String())
	conn, connExists := h.requestIdToConnMap[requestId]
	if !connExists {
		errMessage := fmt.Sprintf("Connection not present for requestId: %s", requestId.String())
		h.ctx.Logger.Printf(errMessage)
		return nil, errors.New(errMessage)
	}
	delete(h.requestIdToConnMap, requestId)
	h.ctx.Logger.Printf("Successfully fetched connection for requestId: %s", requestId.String())
	return &conn, nil
}

func (h *ConnectionHandler) processEventForConnection(connFd int) {
	conn, isConnPresent := h.connectionFileDescriptorBiMap.Lookup(connFd)
	if !isConnPresent {
		h.ctx.Logger.Printf("Connection object not found for connection file descriptor: %d. Terminating the connection", connFd)
		h.terminateConnection(conn)
		return
	}
	dataFromConn, err := h.fetchDataFromConnection(conn)
	if err != nil {
		h.terminateConnection(conn)
		return
	}
	if len(dataFromConn) == 0 {
		return
	}
	requestId := uuid.New()
	h.ctx.Logger.Printf("From connection (%s) received request ID '%s' with data: %q", conn.RemoteAddr(), requestId.String(), dataFromConn)
	h.requestIdToConnMap[requestId] = conn
	response := h.requestHandler.ProcessRequest(constants.Request{
		Data:      dataFromConn,
		RequestId: requestId,
	})
	h.writeDataToConnection(conn, response)
}

func (h *ConnectionHandler) acceptConnections() {
	for {
		conn, err := h.ctx.ServerInstance.Listener.Accept()
		if err != nil {
			h.ctx.Logger.Println("Error accepting connection: ", err.Error())
			continue
		}

		connFd := h.getConnectionFileDescriptor(conn)
		h.ctx.Logger.Printf("For connection (%s) Got connection file descriptor: %d", conn.RemoteAddr(), connFd)

		h.connectionFileDescriptorBiMap.Insert(connFd, conn)
		err = syscall.EpollCtl(h.epollFd, syscall.EPOLL_CTL_ADD, connFd, &syscall.EpollEvent{
			Events: syscall.EPOLLIN,
			Fd:     int32(connFd),
		})
		if err != nil {
			h.ctx.Logger.Printf("Error adding connection (%s) to epoll: %v", conn.RemoteAddr(), err.Error())
			h.terminateConnection(conn)
			continue
		}

		h.ctx.Logger.Printf("Connection with (%s) successfully established", conn.RemoteAddr())
	}
}

func (h *ConnectionHandler) fetchDataFromConnection(conn net.Conn) ([]byte, error) {
	// h.ctx.Logger.Printf("Started reading data from connection (%s)", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	data := make([]byte, 0, 1024)
	for {
		readBuf := make([]byte, 1024)
		bytesRead, err := reader.Read(readBuf)
		// h.ctx.Logger.Printf("From connection (%s) read %d bytes", conn.RemoteAddr(), bytesRead)
		if err != nil {
			// h.ctx.Logger.Printf("From connection (%s) error reading data from: %v", conn.RemoteAddr(), err.Error())
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
		// h.ctx.Logger.Printf("From connection (%s) no data received", conn.RemoteAddr())
	}

	return data, nil
}

func (h *ConnectionHandler) handleMasterConnection() {
	hostServer := h.ctx.ServerInstance
	serverRole := hostServer.ReplicationConfig.Role
	if serverRole == constants.MASTER_ROLE {
		return
	}
	masterAddress := hostServer.ReplicationConfig.MasterServerAddress
	masterConn, err := h.initiateHandShakeWithMaster(hostServer)
	if err != nil {
		h.ctx.Logger.Fatalf("Error while trying to initiate handshake with master (%s): %v", masterAddress, err.Error())
	}

	h.ctx.Logger.Printf("Handshake completed with master at address: %s", masterAddress)
	masterConnFd := h.getConnectionFileDescriptor(masterConn)
	h.ctx.Logger.Printf("For master connection (%s) Got connection file descriptor: %d", masterAddress, masterConnFd)

	h.connectionFileDescriptorBiMap.Insert(masterConnFd, masterConn)
	err = syscall.EpollCtl(h.epollFd, syscall.EPOLL_CTL_ADD, masterConnFd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(masterConnFd),
	})
	if err != nil {
		h.ctx.Logger.Fatalf("[FATAL] Error adding master connection (%s) to epoll: %v", masterAddress, err.Error())
		h.terminateConnection(masterConn)
	}

	h.ctx.Logger.Printf("Connection with master (%s) successfully established", masterConn.RemoteAddr())
}

func (h *ConnectionHandler) initiateHandShakeWithMaster(serverInstance *server.Server) (net.Conn, error) {
	masterServerAddress := (*serverInstance).ReplicationConfig.MasterServerAddress
	conn, err := net.Dial("tcp", masterServerAddress)

	if err != nil {
		h.ctx.Logger.Printf("Error while trying to establish connection with master (%s): %v", masterServerAddress, err.Error())
		return nil, err
	}
	handshakePipeline := h.createHandshakePipeline(serverInstance)
	for _, handshakeStep := range handshakePipeline {
		cmd := handshakeStep.CommandName
		h.ctx.Logger.Printf("Beginning the handshake step with master (%s) using command (%s)", masterServerAddress, cmd)

		err := h.writeDataToConnection(conn, []constants.DataRepr{handshakeStep.Request})
		if err != nil {
			h.ctx.Logger.Printf("Error while trying to send command (%s) master (%s): %v", cmd, masterServerAddress, err.Error())
			return nil, err
		}

		responseFromMaster, err := h.fetchDataFromConnection(conn)
		if err != nil {
			h.ctx.Logger.Printf("Error while waiting for response from master (%s) for command (%s): %v", masterServerAddress, cmd, err.Error())
			return nil, err
		}
		if len(responseFromMaster) == 0 {
			errMessage := fmt.Sprintf("Empty response received from master (%s) for command (%s): %v", masterServerAddress, cmd, err.Error())
			h.ctx.Logger.Printf(errMessage)
			return nil, errors.New(errMessage)
		}
		h.ctx.Logger.Printf("Received response from master (%s) for command (%s): %q", masterServerAddress, cmd, responseFromMaster)

		decodedResponseFromMaster, err := parser.Decode(responseFromMaster)
		if err != nil {
			h.ctx.Logger.Printf("Error while decoding for response from master (%s) for command (%s): %v", masterServerAddress, cmd, err.Error())
			return nil, err
		}
		prefixOnlyCheck := false
		if cmd == constants.PSYNC_COMMAND {
			prefixOnlyCheck = true
		}
		if !decodedResponseFromMaster.IsEqual(handshakeStep.ExpectedResponse, prefixOnlyCheck) {
			h.ctx.Logger.Printf("Unexpected response from master (%s) for command (%s): %+v", masterServerAddress, cmd, decodedResponseFromMaster)
			return nil, err
		}
	}
	return conn, nil
}

func (h *ConnectionHandler) createHandshakePipeline(serverInstance *server.Server) []HandshakeStep {
	handshakePipeline := []HandshakeStep{}
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.PING_COMMAND,
		Request:          utils.CreateRequestForCommand(constants.PING_COMMAND),
		ExpectedResponse: utils.CreateStringResponse(constants.PONG_RESPONSE),
	})
	ok_response := utils.CreateStringResponse(constants.OK_RESPONSE)
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.REPLCONF_COMMAND,
		Request:          utils.CreateRequestForCommand(constants.REPLCONF_COMMAND, constants.REPLCONF_LISTENING_PORT_PARAM, (serverInstance).ListeningPort),
		ExpectedResponse: ok_response,
	})
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.REPLCONF_COMMAND,
		Request:          utils.CreateRequestForCommand(constants.REPLCONF_COMMAND, constants.REPLCONF_CAPA_PARAM, constants.REPLCONF_PSYNC2_PARAM),
		ExpectedResponse: ok_response,
	})
	handshakePipeline = append(handshakePipeline, HandshakeStep{
		CommandName:      constants.PSYNC_COMMAND,
		Request:          utils.CreateRequestForCommand(constants.PSYNC_COMMAND, constants.PSYNC_UNKNOWN_REPLICATION_ID_PARAM, constants.PSYNC_UNKNOWN_MASTER_OFFSET),
		ExpectedResponse: utils.CreateStringResponse(constants.FULLRESYNC_RESPONSE),
	})

	return handshakePipeline
}

func (h *ConnectionHandler) getConnectionFileDescriptor(conn net.Conn) int {
	tcpConn := conn.(*net.TCPConn)
	file, err := tcpConn.File()

	if err != nil {
		h.ctx.Logger.Fatalf("Error getting file descriptor: %v", err.Error())
	}
	return int(file.Fd())
}

func (h *ConnectionHandler) terminateConnection(conn net.Conn) {
	h.closeConnection(conn)
	h.ctx.ConnectionClosedNotificationChan <- conn
	connFd, connFdPresent := h.connectionFileDescriptorBiMap.ReverseLookup(conn)
	if !connFdPresent {
		h.ctx.Logger.Printf(" Connection (%s) file descriptor not found in connection file descriptor map while terminating connection", conn.RemoteAddr())
		return
	}

	h.closeConnectionPoll(connFd)
}

func (h *ConnectionHandler) closeConnectionPoll(connFd int) {
	err := syscall.EpollCtl(h.epollFd, syscall.EPOLL_CTL_DEL, connFd, nil)
	if err != nil {
		h.ctx.Logger.Printf("Failed to remove fd %d from epoll: %v", connFd, err)
	}
	h.connectionFileDescriptorBiMap.Delete(connFd)
	h.ctx.Logger.Printf("[ConnFD-%d] Connection closed", connFd)
}

func (h *ConnectionHandler) closeConnection(conn net.Conn) {
	err := conn.Close()
	if err != nil {
		h.ctx.Logger.Printf("(%s) Error closing connection: '%v'", conn.RemoteAddr(), err.Error())
		os.Exit(1)
	}
	h.connectionFileDescriptorBiMap.DeleteUsingReverseLookup(conn)
	h.ctx.Logger.Printf("(%s) Connection closed", conn.RemoteAddr())
}

func (h *ConnectionHandler) writeDataToConnection(conn net.Conn, dataList []constants.DataRepr) error {
	for _, data := range dataList {
		encodedData := parser.Encode(data)
		h.ctx.Logger.Printf("(%s) Begin writing data '%q' to connection", conn.RemoteAddr(), encodedData)
		_, err := conn.Write(encodedData)
		if err != nil {
			h.ctx.Logger.Printf("(%s) Error writing data '%q' to connection: %v", conn.RemoteAddr(), encodedData, err.Error())
			return err
		}
		h.ctx.Logger.Printf("(%s) Successfully written data '%q' to connection", conn.RemoteAddr(), encodedData)
	}
	return nil
}
