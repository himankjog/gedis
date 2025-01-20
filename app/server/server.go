package server

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

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

type ServerConfig struct {
	RdbDir     string
	DbFileName string
}

type Server struct {
	ListeningPort     string
	Listener          net.Listener
	ReplicaOf         string
	ReplicationConfig ServerReplicationConfig
	ServerAddress     string
	ServerConfig      ServerConfig
}

var (
	serverInstance *Server
	once           sync.Once
)

func GetServerInstance() *Server {
	once.Do(func() {
		serverInstance = initializeServer()
	})
	return serverInstance

}

func (s *Server) GetRdbDir() string {
	return s.ServerConfig.RdbDir
}

func (s *Server) GetRdbFileName() string {
	return s.ServerConfig.DbFileName
}

func initializeServer() *Server {
	serverObj := Server{}
	port := flag.String("port", constants.DEFAULT_SERVER_PORT, "Gedis listening port")
	replicaof := flag.String("replicaof", "", "Master server address")
	dir := flag.String("dir", "", "RDB File directory")
	dbFileName := flag.String("dbfilename", "", "RDB file name")
	flag.Parse()

	serverObj.ListeningPort = *port
	serverObj.ReplicaOf = *replicaof

	serverRole := constants.MASTER_ROLE
	masterServerAddress := ""
	if len(serverObj.ReplicaOf) != 0 {
		serverRole = constants.REPLICA_ROLE
		masterServerAddress = strings.Replace(serverObj.ReplicaOf, " ", ":", 1)
	}

	serverObj.ReplicationConfig = ServerReplicationConfig{
		Role:                serverRole,
		MasterServerAddress: masterServerAddress,
	}

	serverObj.ServerConfig = ServerConfig{
		RdbDir:     *dir,
		DbFileName: *dbFileName,
	}

	serverObj.ServerAddress = fmt.Sprintf("%s:%s", constants.DEFAULT_SERVER_ADDRESS, serverObj.ListeningPort)
	serverObj.Listener = getListener(serverObj.ServerAddress)
	// TODO: Update following configurations once communication with master is established
	serverObj.ReplicationConfig.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverObj.ReplicationConfig.MasterReplOffset = 0

	log.Printf("[%s] Server state: %+v", serverObj.ServerAddress, serverObj)
	return &serverObj
}

func getListener(address string) net.Listener {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to bind to port on address %s: %v", address, err)
	}
	return l
}
