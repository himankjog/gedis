package main

import (
	"fmt"
	"net"
	"os"
)

func closeConnection(conn net.Conn) {
	err := conn.Close()
	if err != nil {
		fmt.Println("Error closing connection: ", err.Error())
		os.Exit(1)
	}
}

func main() {
	conn, err := getPortConn()

	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	buf := make([]byte, 1024)
	for {
		input, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading data: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Received: ", string(buf[:input]))
		sendPongResponse(conn, err)
	}
}

func getPortConn() (net.Conn, error) {
	address := "0.0.0.0:6379"
	listener, err := getPortListener(address)
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Failed to create connection on address ", address)
		os.Exit(1)
	}
	defer closeConnection(conn)
	return conn, err
}

func getPortListener(address string) (net.Listener, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port on address ", address)
		os.Exit(1)
	}
	return l, err
}

func sendPongResponse(conn net.Conn, err error) {
	_, err = conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
}
