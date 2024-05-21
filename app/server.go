package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

const (
	ADDRESS = "0.0.0.0:6379"
	PONG    = "+PONG\r\n"
)

func main() {
	listener := getListener(ADDRESS)
	defer listener.Close()

	log.Println("Listening on address: ", ADDRESS)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer closeConnection(conn)
	log.Println("Connection accepted from ", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		request, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading data from %s: %v", conn.RemoteAddr(), err.Error())
			return
		}
		log.Printf("Received data from %s: %q", conn.RemoteAddr(), request)

		if request == "PING" {
			sendPongResponse(conn)
		}
	}
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
