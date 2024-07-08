package rest

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
)

func InitServer(host string, port int) {
	r := gin.Default()
	r.GET("/transform", func(ctx *gin.Context) {
		TransformRequest(ctx)
	})

	r.GET("/transform/arrow", func(ctx *gin.Context) {
		ArrowTransformRequest(ctx)
	})

	log.Printf("strings REST server on %s:%d", host, port)
	r.Run(fmt.Sprintf("%s:%d", host, port))
}
