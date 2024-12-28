package server

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
)

type ServerReplicationConfig struct {
	Role                       string
	ConnectedSlaves            int
	MasterReplId               string
	MasterReplOffset           int
	SecondReplOffeset          int
	ReplBacklogActive          int
	ReplBacklogSize            int
	ReplBacklogFirstByteOffset int
	ReplBacklogHistlen         int
	MasterServerAddress        string
}

type Server struct {
	ListeningPort     string
	Listener          net.Listener
	ReplicaOf         string
	ReplicationConfig ServerReplicationConfig
	ServerAddress     string
}

func StartServer() *Server {
	return initializeServer()

}

func initializeServer() *Server {
	serverInstance := Server{}
	port := flag.String("port", constants.DEFAULT_SERVER_PORT, "Gedis listening port")
	replicaof := flag.String("replicaof", "", "Master server address")
	flag.Parse()

	serverInstance.ListeningPort = *port
	serverInstance.ReplicaOf = *replicaof

	serverRole := constants.MASTER_ROLE
	masterServerAddress := ""
	if len(serverInstance.ReplicaOf) != 0 {
		serverRole = constants.REPLICA_ROLE
		masterServerAddress = strings.Replace(serverInstance.ReplicaOf, " ", ":", 1)
	}

	serverInstance.ReplicationConfig = ServerReplicationConfig{
		Role:                serverRole,
		MasterServerAddress: masterServerAddress,
	}

	serverInstance.ServerAddress = fmt.Sprintf("%s:%s", constants.DEFAULT_SERVER_ADDRESS, serverInstance.ListeningPort)
	serverInstance.Listener = getListener(serverInstance.ServerAddress)
	// TODO: Update following configurations once communication with master is established
	serverInstance.ReplicationConfig.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverInstance.ReplicationConfig.MasterReplOffset = 0

	log.Printf("[%s] Server state: %+v", serverInstance.ServerAddress, serverInstance)
	return &serverInstance
}

func getListener(address string) net.Listener {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port on address %s: %v", address, err)
	}
	return l
}
