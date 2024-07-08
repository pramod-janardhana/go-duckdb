package websocket

import (
	"fmt"
	"go-duckdb/config"
	querybuilder "go-duckdb/internal/query_builder"
	"log"
	"path"
	"time"

	utilsQuery "go-duckdb/internal/utils/query"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func TransformRequest(c *gin.Context, ws *websocket.Conn) {
	const (
		tableName = "loadtest"
		viewName  = "v_loadtest"
	)

	filePath := config.WEB_SOCKET.DatasetPath
	p := path.Join(config.WEB_SOCKET.DuckDBDir, fmt.Sprintf("data-%s.duckdb", time.Now().String()))
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

	limit, offset := config.WEB_SOCKET.ChunkSize, 0
	sequencyNumber := 1
	for {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit, offset)
		q, err := utilsQuery.Transform(qb, query)
		if err != nil {
			log.Printf("error getting data, err: %v\n", err)
			return
		}

		if len(q.Data.Rows) < limit {
			log.Printf("breaking because got %d rows out of %d", len(q.Data.Rows), limit)
			break
		}

		q.SequencyNumber = int32(sequencyNumber)
		q.Count = int32(len(q.Data.Rows))

		if err := ws.WriteJSON(q); err != nil {
			log.Printf("error streaming data, err: %v\n", err)
			return
		}

		offset += limit
		sequencyNumber += 1
	}
}
