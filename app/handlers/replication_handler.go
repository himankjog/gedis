package handlers

import (
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Replica struct {
	offset   int
	isActive bool
}

type ReplicationHandler struct {
	ctx            *context.Context
	replicas       map[net.Conn]Replica
	connHandler    *ConnectionHandler
	replicaMapLock sync.Mutex
}

func InitReplicationHandler(appContext *context.Context, connectionHandler *ConnectionHandler) *ReplicationHandler {
	replicationHandler := ReplicationHandler{
		ctx:            appContext,
		replicas:       make(map[net.Conn]Replica),
		connHandler:    connectionHandler,
		replicaMapLock: sync.Mutex{},
	}
	return &replicationHandler
}

func (h *ReplicationHandler) StartReplicationHandler() {
	go h.handleCmdExecutedNotifications()
	go h.cleanUpStaleConnections()
}

func (h *ReplicationHandler) handleCmdExecutedNotifications() {
	for cmdExecutedNotification := range h.ctx.CommandExecutedNotificationChan {
		if h.ctx.ServerInstance.ReplicationConfig.Role != constants.MASTER_ROLE {
			// Only master should cater to these notifications, at least for now
			continue
		}
		if !cmdExecutedNotification.Success {
			h.ctx.Logger.Printf("Not handling command [%s] for replication as it wasn't successfully executed", cmdExecutedNotification.Cmd)
			continue
		}
		switch cmdExecutedNotification.Cmd {
		case constants.PSYNC_COMMAND:
			go h.handleHandshakeWithReplica(cmdExecutedNotification)
		case constants.SET_COMMAND, constants.SET_PX_COMMAND:
			go h.relayCommandToReplica(cmdExecutedNotification)
		}
	}
}

func (h *ReplicationHandler) handleHandshakeWithReplica(cmdExecutedNotification constants.CommandExecutedNotification) {
	h.ctx.Logger.Printf("Handshake completed with replica, adding a new replica")
	conn, err := h.connHandler.GetConnectionForRequest(cmdExecutedNotification.RequestId)
	if err != nil {
		h.ctx.Logger.Printf("Error while trying to fetch connection to requestId: %s", cmdExecutedNotification.RequestId.String())
		return
	}
	currDir, _ := os.Getwd()
	rdbFilePath := filepath.Join(currDir, "app", "persistence", "storage", "empty_hex.rdb")
	binaryDecodedDataFromFile, err := utils.ReadHexFileToBinary(rdbFilePath)

	if err != nil {
		h.ctx.Logger.Printf("Error while trying to decode data from rdb file at path '%s': %v", rdbFilePath, err.Error())
		return
	}
	rdbDecodedData := utils.CreateRdbFileResponse(binaryDecodedDataFromFile)
	err = h.connHandler.writeDataToConnection(*conn, []constants.DataRepr{rdbDecodedData})
	if err != nil {
		h.ctx.Logger.Printf("Error while tyring to send RDB file to replica: %v", err.Error())
		return
	}
	h.replicaMapLock.Lock()
	defer h.replicaMapLock.Unlock()
	h.replicas[*conn] = Replica{
		isActive: true,
		//TODO: Update offset based on data from replica
		offset: 0,
	}
	// TODO: Send all existing writes from offset to current time to newly added Replica
}

func (h *ReplicationHandler) relayCommandToReplica(cmdExecutedNotification constants.CommandExecutedNotification) {
	cmd := cmdExecutedNotification.Cmd
	h.ctx.Logger.Printf("Relaying command [%s] to replicas", cmd)

	h.replicaMapLock.Lock()
	defer h.replicaMapLock.Unlock()
	for conn, replica := range h.replicas {
		if !replica.isActive {
			h.ctx.Logger.Printf("Not relaying command [%s] to replica with address '%s' as connection is not active", cmd, conn.RemoteAddr())
			continue
		}
		err := h.connHandler.writeDataToConnection(conn, []constants.DataRepr{cmdExecutedNotification.DecodedRequest})
		if err != nil {
			h.ctx.Logger.Printf("Error while trying to relay command [%s] to replica with address '%s': %s", cmd, conn.RemoteAddr(), err.Error())
			continue
		}
	}

	h.ctx.Logger.Printf("Successfully relayed command [%s] to all the replicas", cmd)
}

func (h *ReplicationHandler) cleanUpStaleConnections() {
	for closedConn := range h.ctx.ConnectionClosedNotificationChan {
		if h.ctx.ServerInstance.ReplicationConfig.Role != constants.MASTER_ROLE {
			// Only master should cater to these notifications, at least for now
			continue
		}
		h.replicaMapLock.Lock()
		defer h.replicaMapLock.Unlock()
		delete(h.replicas, closedConn)
	}
}
