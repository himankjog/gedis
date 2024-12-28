package main

import (
	"log"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

func main() {
	serverInstance := server.StartServer()
	defer (*serverInstance).Listener.Close()

	log.Printf("Listening on address: %s", (*serverInstance).ServerAddress)
	log.Printf("Replication role: %s", (*serverInstance).ReplicationConfig.Role)

	handlers.AddServer(serverInstance)
	handlers.StartEventLoop(serverInstance)
}
