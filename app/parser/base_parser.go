package parser

import "github.com/codecrafters-io/redis-starter-go/app/context"

var ctx *context.Context

func InitBaseParser(appContext *context.Context) {
	ctx = appContext
}
