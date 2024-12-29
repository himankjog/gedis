package handlers

import "github.com/codecrafters-io/redis-starter-go/app/context"

var ctx *context.Context

func InitializeBaseHandler(appContext *context.Context) {
	ctx = appContext
}
