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
	// Initialize persistence layer
	persiDb := persistence.Init(appContext)

	// Initialize components with context
	notificationHandler := handlers.NewNotificationHandler(appContext)
	commandHandler := handlers.InitCommandHandler(appContext, notificationHandler, persiDb)
	requestHandler := handlers.InitRequestHandler(appContext, commandHandler)
	connectionHandler := handlers.InitConnectionHandler(appContext, requestHandler, notificationHandler)
	replicationHandler := handlers.InitReplicationHandler(appContext, connectionHandler, notificationHandler)

	utils.InitUtils(appContext)
	parser.InitBaseParser(appContext)

	replicationHandler.StartReplicationHandler()
	connectionHandler.StartEventLoop()
}
