package main

import (
	"go-duckdb/internal/services/grpc"
	grpcArrow "go-duckdb/internal/services/grpc_arrow"
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

	// starting gRPC server for arrow
	{
		const (
			host = "localhost"
			port = 9003
		)

		go grpcArrow.InitServer(host, port)
	}

	wg := &sync.WaitGroup{}
	wg.Add(3)
	wg.Wait()
}
