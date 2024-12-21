package handlers

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/utils/bimap"
	"github.com/google/uuid"
)

var CONN_FILE_DESC_BI_MAP = bimap.New[int, net.Conn]()
var EPOLL_FD = -1

func StartEventLoop(listener net.Listener) {
	var err error
	EPOLL_FD, err = syscall.EpollCreate1(0)

	if err != nil {
		log.Fatalf("Error creating epoll: %v", err.Error())
	}

	go acceptConnections(listener)

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
				log.Printf("Connection object not found for connection file descriptor: %d. Stopping the event polling for this.", connFd)
				closeConnectionPoll(connFd)
				continue
			}
			handleConnection(conn)
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

func handleConnection(conn net.Conn) {
	log.Println("[%s] Started handling connection", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	requestData := make([]byte, 0, 1024)
	for {
		readBuf := make([]byte, 1024)
		bytesRead, err := reader.Read(readBuf)
		log.Printf("[%s] Read %d bytes from connection", conn.RemoteAddr(), bytesRead)
		if err != nil {
			fmt.Printf("[%s] Error reading data from: %v", conn.RemoteAddr(), err.Error())
			if err != io.EOF {
				terminateConnection(conn)
			}
			break
		}
		requestData = append(requestData, readBuf[:bytesRead]...)
		if bytesRead < 1024 {
			// If we don't have anymore data to read, we can safely quit the read loop
			break
		}
	}
	requestId := uuid.New()
	log.Printf("[%s] Received request ID '%s' with data: %q", conn.RemoteAddr(), requestId.String(), requestData)
	response, err := ProcessRequest(Request{
		Data:      requestData,
		RequestId: requestId,
	})
	if err != nil {
		log.Printf("[%s] Unable to handle request ID '%s' with data: %v", conn.RemoteAddr(), requestId.String(), requestData)
		terminateConnection(conn)
		return
	}

	sendResponse(conn, response)
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
	log.Printf("[%s] Connection closed", conn)
}

func sendResponse(conn net.Conn, response []byte) {
	_, err := conn.Write(response)
	if err != nil {
		log.Printf("[%s] Error writing response to connection: %v", conn.RemoteAddr(), err.Error())
		return
	}
	log.Printf("[%s] Successfully sent ", conn.RemoteAddr())
}

func sendPongResponse(conn net.Conn) {
	_, err := conn.Write([]byte(PONG))
	if err != nil {
		log.Printf("Error writing data to conn %s: %v", conn.RemoteAddr(), err.Error())
		return
	}
	log.Printf("Sent PONG to %s", conn.RemoteAddr())
}
