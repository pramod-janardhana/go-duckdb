package config

type serviceConfig struct {
	Host        string
	Port        int
	DatasetPath string
	ProfDir     string
	DuckDBDir   string
	ChunkSize   int
}

var REST = serviceConfig{
	Host:        "localhost",
	Port:        9000,
	DatasetPath: "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/dataset/1K.csv",
	ProfDir:     "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/prof",
	DuckDBDir:   "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/duckdb",
	ChunkSize:   50000,
}

var WEB_SOCKET = serviceConfig{
	Host:        "localhost",
	Port:        9001,
	DatasetPath: "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/dataset/20M.csv",
	ProfDir:     "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/prof",
	DuckDBDir:   "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/duckdb",
	ChunkSize:   25000,
}

var GRPC = serviceConfig{
	Host:        "localhost",
	Port:        9002,
	DatasetPath: "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/dataset/20M.csv",
	ProfDir:     "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/prof",
	DuckDBDir:   "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/duckdb",
	ChunkSize:   50000,
}
