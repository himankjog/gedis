package persistence

import "github.com/codecrafters-io/redis-starter-go/app/context"

var ctx *context.Context

func InitBasePersistence(appContext *context.Context) {
	ctx = appContext
}
