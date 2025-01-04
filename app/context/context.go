package context

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

type Context struct {
	ServerInstance                   *server.Server
	Logger                           *log.Logger
	CommandExecutedNotificationChan  chan constants.CommandExecutedNotification
	ConnectionClosedNotificationChan chan constants.ConnectionClosedNotification
}

var context *Context
var once sync.Once

func BuildContext(serverInstance *server.Server) *Context {
	once.Do(func() {
		prefixString := fmt.Sprintf("[%s]", (*serverInstance).ServerAddress)
		logger := log.New(os.Stdout, prefixString, log.Ldate|log.Ltime)
		commandExecutedNotificationChan := make(chan constants.CommandExecutedNotification)
		connectionClosedNotificationChan := make(chan constants.ConnectionClosedNotification)
		context = &Context{
			ServerInstance:                   serverInstance,
			Logger:                           logger,
			CommandExecutedNotificationChan:  commandExecutedNotificationChan,
			ConnectionClosedNotificationChan: connectionClosedNotificationChan,
		}
	})
	return context
}
