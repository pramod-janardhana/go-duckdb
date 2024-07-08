package websocket

import (
	"fmt"
	"go-duckdb/config"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func InitServer(host string, port int) {
	r := gin.Default()
	r.GET("/transform", func(ctx *gin.Context) {
		defer func() {
			log.Println("computed transform")
		}()

		// mem profiler
		{
			p := path.Join(config.WEB_SOCKET.ProfDir, fmt.Sprintf("ws-mem-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
			f, err := os.Create(p)
			if err != nil {
				fmt.Printf("error creating cpu profiler, err: %v\n", err)
				return

			}
			defer pprof.WriteHeapProfile(f)

		}

		// cpu profiler
		{
			p := path.Join(config.WEB_SOCKET.ProfDir, fmt.Sprintf("ws-cpu-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
			f, err := os.Create(p)
			if err != nil {
				fmt.Printf("error creating cpu profiler, err: %v\n", err)
				return

			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		ws, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer ws.Close()

		TransformRequest(ctx, ws)
	})

	log.Printf("strings web socket server on %s:%d", host, port)
	r.Run(fmt.Sprintf("%s:%d", host, port))
}
