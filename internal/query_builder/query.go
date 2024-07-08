package querybuilder

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/marcboeker/go-duckdb"
)

const (
	DEFAULT_PATH = "./data.duckdb"
)

type DuckDBQueryBuilder struct {
	con       *sql.DB
	connector *duckdb.Connector
}

func NewDuckDBQueryBuilder(path string) (*DuckDBQueryBuilder, error) {
	if len(path) == 0 {
		path = DEFAULT_PATH
	}

	con, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(con)

	return &DuckDBQueryBuilder{con: db, connector: con}, nil
}

func (qb DuckDBQueryBuilder) CSVToTable(tableName, filePath string) error {
	return qb.Exec(fmt.Sprintf(`CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv('%s');`, tableName, filePath))
}

func (qb DuckDBQueryBuilder) Exec(query string) error {
	_, err := qb.con.Exec(query)
	return err
}

func (qb DuckDBQueryBuilder) Query(query string) (*sql.Rows, error) {
	return qb.con.Query(query)
}

func (qb DuckDBQueryBuilder) Close() error {
	return qb.con.Close()
}

func (qb DuckDBQueryBuilder) GetArrow(ctx context.Context) (*DuckDBArrowQueryBuilder, error) {
	return NewDuckDBArrowQueryBuilder(ctx, qb.connector)
}
