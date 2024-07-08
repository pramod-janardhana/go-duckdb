package main

import (
	"fmt"
	pb "go-duckdb/internal/services/grpc/data_transform"
	"log"
	"net"

	grpc "google.golang.org/grpc"
)

type DataTransform struct{}

const (
	PORT = 9002
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", PORT))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDataTransformServer(grpcServer, NewDataTransformService())
	grpcServer.Serve(lis)
}
