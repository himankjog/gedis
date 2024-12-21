package main

import (
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
)

const (
	ADDRESS = "0.0.0.0:6379"
)

func main() {
	listener := getListener(ADDRESS)
	defer listener.Close()

	log.Println("Listening on address: ", ADDRESS)

	handlers.StartEventLoop(listener)
}

func getListener(address string) net.Listener {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port on address %s: %v", address, err)
	}
	return l
}
