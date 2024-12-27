package server

import (
	"flag"
	"fmt"
	"log"
	"net"
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
}

type Server struct {
	ListeningPort     string
	Listener          net.Listener
	ReplicaOf         string
	ReplicationConfig ServerReplicationConfig
	ServerAddress     string
}

const (
	DEFAULT_ADDRESS = "0.0.0.0"
	DEFAULT_PORT    = "6379"
	DEFAULT_ROLE    = "master"
	REPLICA_ROLE    = "slave"
)

func StartServer() *Server {
	return initializeServer()

}

func initializeServer() *Server {
	serverInstance := Server{}
	port := flag.String("port", DEFAULT_PORT, "Gedis listening port")
	replicaof := flag.String("replicaof", "", "Master server address")
	flag.Parse()

	serverInstance.ListeningPort = *port
	serverInstance.ReplicaOf = *replicaof

	serverRole := DEFAULT_ROLE
	if len(serverInstance.ReplicaOf) != 0 {
		serverRole = REPLICA_ROLE
	}

	serverInstance.ReplicationConfig = ServerReplicationConfig{
		Role: serverRole,
	}

	serverInstance.ServerAddress = fmt.Sprintf("%s:%s", DEFAULT_ADDRESS, serverInstance.ListeningPort)
	serverInstance.Listener = getListener(serverInstance.ServerAddress)
	// TODO: Update following configurations once communication with master is established
	serverInstance.ReplicationConfig.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverInstance.ReplicationConfig.MasterReplOffset = 0

	return &serverInstance
}

func getListener(address string) net.Listener {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port on address %s: %v", address, err)
	}
	return l
}
