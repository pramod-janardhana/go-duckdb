package query

import (
	"context"
	querybuilder "go-duckdb/internal/query_builder"
	pb "go-duckdb/internal/services/grpc_arrow/data_transform"
	"log"
)

func ArrowTransformV2(qb *querybuilder.DuckDBArrowQueryBuilder, query string) (*pb.QueryOut, error) {
	queryOut := pb.QueryOut{
		SequencyNumber: 1,
		Count:          1,
		Data:           [][]byte{},
	}

	ctx := context.Background()
	defer ctx.Done()

	rows, err := qb.Query(ctx, query)
	if err != nil {
		log.Printf("Error querying data, err: %s\n", err.Error())
		return nil, err
	}

	var count int64 = 0
	for rows.Next() {
		record := rows.Record()
		data, err := record.MarshalJSON()
		if err != nil {
			log.Printf("Error marshaling record, err: %s\n", err.Error())
		}

		queryOut.Data = append(queryOut.Data, data)
		count += record.NumRows()
		record.Release()
	}

	queryOut.Count = int32(count)
	return &queryOut, nil
}
