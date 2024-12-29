package utils

import "github.com/codecrafters-io/redis-starter-go/app/context"

var ctx *context.Context

func InitUtils(appContext *context.Context) {
	ctx = appContext
}
