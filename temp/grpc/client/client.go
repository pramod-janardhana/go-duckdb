package main

import (
	"context"
	pb "go-duckdb/internal/services/grpc/data_transform"
	"io"
	"log"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DataTransform struct{}

const (
	PORT = 9000
)

func main() {
	var opts []grpc.DialOption
	{
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10*1024*1024)))
	}

	conn, err := grpc.Dial("localhost:9002", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	defer conn.Close()
	client := pb.NewDataTransformClient(conn)
	stream, err := client.Transform(context.Background(), &emptypb.Empty{})
	if err != nil {
		log.Fatalf("Error in transform: %v", err)
	}

	for {
		rows, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%v.Listrows(_) = _, %v", client, err)
		}
		log.Println(rows.SequencyNumber, rows.Count)
	}

}
