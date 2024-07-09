package grpc_arrow

import (
	"context"
	"fmt"
	"go-duckdb/config"
	querybuilder "go-duckdb/internal/query_builder"
	pb "go-duckdb/internal/services/grpc_arrow/data_transform"
	"log"
	"os"
	"path"
	"runtime/pprof"
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
	p := path.Join(config.GRPC_ARROW.DuckDBDir, fmt.Sprintf("data-%s.duckdb", time.Now().String()))
	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Fatalf("Error creating query builder, err: %v\n", err)
	}

	return &dataTransform{
		qb: qb,
	}
}

func (t dataTransform) Transform(_ *emptypb.Empty, stream pb.DataTransform_TransformServer) error {
	defer func() {
		log.Println("computed transform")
	}()

	// mem profiler
	{
		p := path.Join(config.GRPC_ARROW.ProfDir, fmt.Sprintf("grpc-arrow-mem-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		defer pprof.WriteHeapProfile(f)

	}

	// cpu profiler
	{
		p := path.Join(config.GRPC_ARROW.ProfDir, fmt.Sprintf("grpc-arrow-cpu-%s.prof", time.Now().UTC().Format("2006-01-02 15:04:05")))
		f, err := os.Create(p)
		if err != nil {
			fmt.Printf("error creating cpu profiler, err: %v\n", err)
			return err

		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()
	arrowQB, err := t.qb.GetArrow(ctx)
	if err != nil {
		log.Fatalf("Error creating arrow query builder, err: %v\n", err)
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("Error closing arrow query builder, err: %v\n", err)
		}
	}()

	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	filePath := config.GRPC_ARROW.DatasetPath
	if err := t.qb.CSVToTable(tableName, filePath); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return err
	}

	if err := t.qb.Exec(utilsQuery.CreateView(viewName, tableName)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return err
	}

	limit, offset := config.GRPC_ARROW.ChunkSize, 0
	sequencyNumber := 1
	for {
		q, err := utilsQuery.ArrowTransformV2(arrowQB, fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit, offset))
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return err
		}

		q.SequencyNumber = int32(sequencyNumber)
		if err := stream.Send(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return err
		}

		if int(q.Count) < limit {
			log.Printf("breaking because got %d rows out of %d", q.Count, limit)
			break
		}

		offset += limit
		sequencyNumber += 1
	}

	return nil
}
