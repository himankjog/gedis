package main

import (
	"log"

	"github.com/codecrafters-io/redis-starter-go/app/context"
	"github.com/codecrafters-io/redis-starter-go/app/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/parser"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

func main() {
	serverInstance := server.GetServerInstance()
	defer (*serverInstance).Listener.Close()

	log.Printf("Listening on address: %s", (*serverInstance).ServerAddress)
	log.Printf("Replication role: %s", (*serverInstance).ReplicationConfig.Role)

	appContext := context.BuildContext(serverInstance)

	// Initialize components with context
	handlers.InitializeBaseHandler(appContext)
	utils.InitUtils(appContext)
	persistence.InitBasePersistence(appContext)
	parser.InitBaseParser(appContext)

	handlers.StartEventLoop()
}
