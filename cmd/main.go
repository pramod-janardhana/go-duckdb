package main

import (
	"go-duckdb/internal/services/grpc"
	"go-duckdb/internal/services/rest"
	"go-duckdb/internal/services/websocket"
	"sync"
)

func main() {
	// starting REST server
	{
		const (
			host = "localhost"
			port = 9000
		)

		go rest.InitServer(host, port)
	}

	// starting Web Socket server
	{
		const (
			host = "localhost"
			port = 9001
		)

		go websocket.InitServer(host, port)
	}

	// starting gRPC server
	{
		const (
			host = "localhost"
			port = 9002
		)

		go grpc.InitServer(host, port)
	}

	wg := &sync.WaitGroup{}
	wg.Add(3)
	wg.Wait()
}
