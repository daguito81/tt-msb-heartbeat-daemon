package dbmgmt

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	_ "github.com/denisenkom/go-mssqldb" // To load libraries

	"github.com/joho/godotenv"
)

var db *sql.DB
var port = 1433

// ConnectDatabase connects to the database based on the envinronment variables
// DB_SERVER, DB_USER, DB_PASSWORd, DB_DATABASE
func ConnectDatabase() (*sql.DB, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}
	var server = os.Getenv("DB_SERVER")
	var user = os.Getenv("DB_USER")
	var password = os.Getenv("DB_PASSWORD")
	var database = os.Getenv("DB_DATABASE")

	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	// Create connection pool
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	log.Println("Database Connected")
	return db, nil
}
