package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
)

const (
	ADDRESS      = "0.0.0.0:6379"
	PONG         = "+PONG\r\n"
	PING_REQUEST = "PING\r\n"
)

var connections = make(map[int]net.Conn)

func main() {
	listener := getListener(ADDRESS)
	defer listener.Close()

	log.Println("Listening on address: ", ADDRESS)

	epollFd, err := syscall.EpollCreate1(0)

	if err != nil {
		log.Fatalf("Error creating epoll: %v", err.Error())
	}

	go acceptConnections(listener, epollFd)

	eventLoop(epollFd)
}

func acceptConnections(listener net.Listener, epollFd int) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue
		}

		connFd := getConnectionFileDescriptor(conn)
		log.Printf("For connection %s fd: %d", conn.RemoteAddr(), connFd)

		connections[connFd] = conn
		err = syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, connFd, &syscall.EpollEvent{
			Events: syscall.EPOLLIN | syscall.EPOLLERR,
			Fd:     int32(connFd),
		})
		if err != nil {
			log.Printf("Error adding connection %s to epoll: %v", conn.RemoteAddr(), err.Error())
			closeConnection(conn)
			closeConnectionPoll(epollFd, connFd)
			continue
		}

		log.Printf("Connection accepted from: %s", conn.RemoteAddr())
	}
}

func getConnectionFileDescriptor(conn net.Conn) int {
	tcpConn := conn.(*net.TCPConn)
	file, err := tcpConn.File()

	if err != nil {
		log.Fatalf("Error getting file descriptor: %v", err.Error())
	}
	return int(file.Fd())
}

func eventLoop(epollFd int) {
	events := make([]syscall.EpollEvent, 100)

	for {
		n, err := syscall.EpollWait(epollFd, events, -1)
		if err != nil {
			log.Fatalf("Error waiting for epoll events: %v", err.Error())
		}
		log.Printf("Epoll has %d events", n)
		for i := 0; i < n; i++ {
			if events[i].Fd == -1 {
				continue
			}
			connFd := int(events[i].Fd)
			conn := connections[connFd]
			handleConnection(conn, epollFd, connFd)
		}
	}
}

func handleConnection(conn net.Conn, epollFd int, connFd int) {
	log.Println("Handling connection from ", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	for {
		request, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading data from %s: %v", conn.RemoteAddr(), err.Error())
			closeConnection(conn)
			closeConnectionPoll(epollFd, connFd)
			break
		}
		log.Printf("Received data from %s: %q", conn.RemoteAddr(), request)

		if request == PING_REQUEST {
			sendPongResponse(conn)
			break
		}
	}
}

func closeConnectionPoll(epollFd int, connFd int) {
	err := syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_DEL, connFd, nil)
	if err != nil {
		log.Printf("Failed to remove fd %d from epoll: %v", connFd, err)
	}
	delete(connections, connFd)
	log.Printf("Connection %d closed", connFd)
}

func closeConnection(conn net.Conn) {
	err := conn.Close()
	if err != nil {
		fmt.Println("Error closing connection: ", err.Error())
		os.Exit(1)
	}
}

func getListener(address string) net.Listener {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port on address %s: %v", address, err)
	}
	return l
}

func sendPongResponse(conn net.Conn) {
	_, err := conn.Write([]byte(PONG))
	if err != nil {
		log.Printf("Error writing data to conn %s: %v", conn.RemoteAddr(), err.Error())
		return
	}
	log.Printf("Sent PONG to %s", conn.RemoteAddr())
}
