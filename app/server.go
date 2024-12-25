package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
)

const (
	ADDRESS = "0.0.0.0"
	PORT    = "6379"
)

func main() {
	port := flag.String("port", PORT, "Gedis listening port")
	flag.Parse()

	serverAddress := fmt.Sprintf("%s:%s", ADDRESS, *port)
	listener := getListener(serverAddress)
	defer listener.Close()

	log.Println("Listening on address: ", serverAddress)

	handlers.StartEventLoop(listener)
}

func getListener(address string) net.Listener {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port on address %s: %v", address, err)
	}
	return l
}
