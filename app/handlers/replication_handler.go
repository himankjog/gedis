package handlers

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Replica struct {
	offset   int
	isActive bool
}

type ReplicationHandler struct {
	ctx                 *context.Context
	replicas            map[net.Conn]*Replica
	connHandler         *ConnectionHandler
	notificationHandler *NotificationHandler
	replicaMapLock      sync.Mutex
}

func InitReplicationHandler(appContext *context.Context, connectionHandler *ConnectionHandler, notificationHandler *NotificationHandler) *ReplicationHandler {
	replicationHandler := ReplicationHandler{
		ctx:                 appContext,
		replicas:            make(map[net.Conn]*Replica),
		connHandler:         connectionHandler,
		notificationHandler: notificationHandler,
		replicaMapLock:      sync.Mutex{},
	}
	notificationHandler.SubscribeToCmdExecutedNotification(replicationHandler.processCmdExecutedNotification)
	notificationHandler.SubscribeToConnClosedNotification(replicationHandler.processConnectionClosedNotification)
	return &replicationHandler
}

func (h *ReplicationHandler) StartReplicationHandler() {
	go h.cleanupInactiveConnections()
	go h.replicaHealthCheck()
}

func (h *ReplicationHandler) processCmdExecutedNotification(notification constants.CommandExecutedNotification) (bool, error) {
	h.ctx.Logger.Printf("Invoked processCmdExecutedNotifications in replication handler")
	if h.ctx.ServerInstance.ReplicationConfig.Role != constants.MASTER_ROLE {
		// Only master should cater to these notifications, at least for now
		return true, nil
	}
	if !notification.Success {
		h.ctx.Logger.Printf("Not handling command [%s] for replication as it wasn't successfully executed", notification.Cmd)
		return true, nil
	}
	// Handle resonses
	switch notification.Cmd {
	case constants.PSYNC_COMMAND:
		return h.handleHandshakeWithReplica(notification)
	case constants.SET_COMMAND, constants.SET_PX_COMMAND:
		return h.relayCommandToReplica(notification)
	default:
		return true, nil
	}
}

func (h *ReplicationHandler) handleHandshakeWithReplica(cmdExecutedNotification constants.CommandExecutedNotification) (bool, error) {
	h.ctx.Logger.Printf("Handshake completed with replica, adding a new replica")
	conn, err := h.connHandler.GetConnectionForRequest(cmdExecutedNotification.RequestId)
	if err != nil {
		h.ctx.Logger.Printf("Error while trying to fetch connection to requestId: %s", cmdExecutedNotification.RequestId.String())
		return false, err
	}
	currDir, _ := os.Getwd()
	rdbFilePath := filepath.Join(currDir, "app", "persistence", "storage", "empty_hex.rdb")
	binaryDecodedDataFromFile, err := utils.ReadHexFileToBinary(rdbFilePath)

	if err != nil {
		h.ctx.Logger.Printf("Error while trying to decode data from rdb file at path '%s': %v", rdbFilePath, err.Error())
		return false, err
	}
	rdbDecodedData := utils.CreateRdbFileResponse(binaryDecodedDataFromFile)
	err = h.connHandler.writeDataToConnection(*conn, []constants.DataRepr{rdbDecodedData})
	if err != nil {
		h.ctx.Logger.Printf("Error while tyring to send RDB file to replica: %v", err.Error())
		return false, err
	}
	h.replicaMapLock.Lock()
	defer h.replicaMapLock.Unlock()
	h.replicas[*conn] = &Replica{
		isActive: true,
		//TODO: Update offset based on data from replica
		offset: 0,
	}
	h.ctx.Logger.Printf("Added replica for connection [%s]. Total count of replicas added = %d", (*conn).RemoteAddr(), len(h.replicas))
	h.ctx.ConnectedReplicasHeartbeatNotificationChan <- constants.ConnectedReplicaHeartbeatNotification{
		ConnectedReplicas: len(h.replicas),
	}
	// TODO: Send all existing writes from offset to current time to newly added Replica
	return true, nil
}

func (h *ReplicationHandler) relayCommandToReplica(cmdExecutedNotification constants.CommandExecutedNotification) (bool, error) {
	cmd := cmdExecutedNotification.Cmd
	h.ctx.Logger.Printf("Relaying command [%s] to replicas", cmd)

	h.replicaMapLock.Lock()
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
	h.replicaMapLock.Unlock()
	h.ctx.Logger.Printf("Successfully relayed command [%s] to all the replicas", cmd)
	return true, nil
}

func (h *ReplicationHandler) processConnectionClosedNotification(notification constants.ConnectionClosedNotification) (bool, error) {
	if h.ctx.ServerInstance.ReplicationConfig.Role != constants.MASTER_ROLE {
		// Only master should cater to these notifications, at least for now
		return true, nil
	}
	h.replicaMapLock.Lock()
	replica := h.replicas[notification.Conn]
	replica.isActive = false
	h.replicaMapLock.Unlock()
	return true, nil
}

func (h *ReplicationHandler) cleanupInactiveConnections() {
	for {
		h.replicaMapLock.Lock()
		for conn, replica := range h.replicas {
			if !replica.isActive {
				delete(h.replicas, conn)
			}
		}
		h.replicaMapLock.Unlock()
		time.Sleep(1 * time.Minute)
	}
}

func (h *ReplicationHandler) replicaHealthCheck() {
	for {
		h.ctx.Logger.Printf("Starting by sending health check to replicas: %d", len(h.replicas))
		h.replicaMapLock.Lock()
		h.ctx.Logger.Printf("Got lock to send health checks")
		for conn, replica := range h.replicas {
			h.ctx.Logger.Printf("Trying to send health check to conn: %s with Replica data: %+v", conn.RemoteAddr(), replica)
			if !replica.isActive {
				h.ctx.Logger.Printf("Not checking health of replica with address '%s' as connection is not active", conn.RemoteAddr())
				continue
			}
			h.ctx.Logger.Printf("Sending health check to replica at address: %s", conn.RemoteAddr())
			h.connHandler.writeDataToConnection(conn, []constants.DataRepr{utils.CreateReplconfGetack(replica.offset)})
		}
		h.ctx.Logger.Printf("Done sending health checks to replicas")
		h.replicaMapLock.Unlock()
		time.Sleep(5 * time.Minute)
	}
}
