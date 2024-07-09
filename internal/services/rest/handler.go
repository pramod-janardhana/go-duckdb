package rest

import (
	"context"
	"fmt"
	"go-duckdb/config"
	querybuilder "go-duckdb/internal/query_builder"
	"go-duckdb/internal/services/grpc/data_transform"
	"log"
	"net/http"
	"path"
	"time"

	utilsQuery "go-duckdb/internal/utils/query"

	"github.com/gin-gonic/gin"
)

func TransformRequest(c *gin.Context) {
	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	filePath := config.REST.DatasetPath
	p := path.Join(config.REST.DuckDBDir, fmt.Sprintf("data-%s.duckdb", time.Now().String()))
	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Printf("Error creating query builder, err: %v\n", err)
		return
	}

	if err := qb.CSVToTable(tableName, filePath); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return
	}

	if err := qb.Exec(utilsQuery.CreateView(viewName, tableName)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return
	}

	limit, offset := config.REST.ChunkSize, 0
	sequencyNumber := 1

	toReturn := []*data_transform.QueryOut{}
	for {
		q, err := utilsQuery.Transform(qb, fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit, offset))
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return
		}

		q.SequencyNumber = int32(sequencyNumber)
		q.Count = int32(len(q.Data.Rows))

		toReturn = append(toReturn, q)

		if len(q.Data.Rows) < limit {
			log.Printf("breaking because got %d rows out of %d", len(q.Data.Rows), limit)
			break
		}

		offset += limit
		sequencyNumber += 1
	}

	c.JSON(http.StatusOK, toReturn)
}

func ArrowTransformRequest(c *gin.Context) {
	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	filePath := config.REST.DatasetPath
	p := path.Join(config.REST.DuckDBDir, fmt.Sprintf("data-%s.duckdb", time.Now().String()))
	qb, err := querybuilder.NewDuckDBQueryBuilder(p)
	if err != nil {
		log.Printf("Error creating query builder, err: %v\n", err)
		return
	}

	ctx := context.Background()
	arrowQB, err := qb.GetArrow(ctx)
	if err != nil {
		log.Printf("Error creating arrow query builder, err: %v\n", err)
		return
	}

	defer func() {
		if err := arrowQB.Close(); err != nil {
			log.Printf("Error closing arrow query builder, err: %v\n", err)
		}
	}()

	if err := qb.CSVToTable(tableName, filePath); err != nil {
		log.Printf("error loading data to duck-db, err: %v\n", err)
		return
	}

	if err := qb.Exec(utilsQuery.CreateView(viewName, tableName)); err != nil {
		log.Printf("error creating view, err: %v\n", err)
		return
	}

	limit, offset := config.REST.ChunkSize, 0
	sequencyNumber := 1

	toReturn := []*utilsQuery.ArrowQueryOut{}
	for {
		q, err := utilsQuery.ArrowTransform(arrowQB, fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit, offset))
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return
		}

		q.SequencyNumber = int32(sequencyNumber)
		q.Count = int32(len(q.Data.Rows))

		toReturn = append(toReturn, q)

		if len(q.Data.Rows) < limit {
			log.Printf("breaking because got %d rows out of %d", len(q.Data.Rows), limit)
			break
		}

		offset += limit
		sequencyNumber += 1
	}

	c.JSON(http.StatusOK, toReturn)
}
