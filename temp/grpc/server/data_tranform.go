package main

import (
	"fmt"
	querybuilder "go-duckdb/internal/query_builder"
	pb "go-duckdb/internal/services/grpc/data_transform"
	"log"
	"time"

	utilsQuery "go-duckdb/internal/utils/query"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// UnimplementedDataTransformServer must be embedded to have forward compatible implementations.
type dataTransform struct {
	pb.UnimplementedDataTransformServer
	qb *querybuilder.DuckDBQueryBuilder
}

func NewDataTransformService() *dataTransform {
	path := fmt.Sprintf("/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/duckdb/data-%s.duckdb", time.Now().String())
	qb, err := querybuilder.NewDuckDBQueryBuilder(path)
	if err != nil {
		log.Fatalf("Error creating query builder, err: %v\n", err)
	}

	return &dataTransform{
		qb: qb,
	}
}

func (t dataTransform) Transform(_ *emptypb.Empty, stream pb.DataTransform_TransformServer) error {
	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
		filePath  = "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/dataset/1K.csv"
	)

	if err := t.qb.CSVToTable(tableName, filePath); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return err
	}

	if err := t.qb.Exec(utilsQuery.CreateView(viewName, tableName)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return err
	}

	limit, offset := 1000, 0
	for {
		q, err := utilsQuery.Transform(t.qb, fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", viewName, limit, offset))
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return err
		}

		if len(q.Data.Rows) < limit {
			log.Printf("breaking because got %d rows out of %d", len(q.Data.Rows), limit)
			break
		}

		if err := stream.Send(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return err
		}

		offset += limit
	}

	return nil
}
