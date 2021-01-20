package dbmgmt

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	_ "github.com/denisenkom/go-mssqldb" // To load libraries

	"github.com/joho/godotenv"
)

// ConnectDatabase connects to the database based on the envinronment variables
// DB_SERVER, DB_USER, DB_PASSWORd, DB_DATABASE
func ConnectDatabase() (*sql.DB, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}
	server := os.Getenv("DB_SERVER")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	database := os.Getenv("DB_DATABASE")
	port := 1433

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

// CountRowsOfDevice returns the number of rows present based on the parameters passed
func countRowsOfDevice(db *sql.DB, clientCode string, deviceCode string, msgCode string) (int, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := fmt.Sprintf("SELECT COUNT(1) FROM device_test WHERE client_code = '%s' AND device_code = '%s' AND msg_code = '%s'",
		clientCode, deviceCode, msgCode)
	rows := db.QueryRowContext(ctx, q)
	var total int
	if err := rows.Scan(&total); err != nil {
		return -1, err
	}
	return total, nil
}

// UpsertDevice will try to find the current device/client/msg combination
// If it already exists, it will update the lastPing time
// If it doesn't exist, it will insert a new row to DB
func UpsertDevice(db *sql.DB, msg *struct {
	clientCode          string
	deviceCode          string
	reportFileProcessed string
	currentTime         string
	msgRaw              bool
	msgCode             string
},
) error {
	t, err := countRowsOfDevice(db, msg.clientCode, msg.deviceCode, msg.msgCode)
	if err != nil {
		return err
	}

	// TODO Implement This
	if t > 0 {
		// Update
	} else {
		// Insert
	}
	return nil
}

func insertDevice(db *sql.DB, clientCode string, deviceCode string, msgCode string) error {
	// TODO Implement This
	return nil
}

func testDatabase(db *sql.DB) {
	// Test Query
	ctx := context.Background()
	tsql := fmt.Sprintf("SELECT * FROM test_table")
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatalln(err)
	}

	for rows.Next() {
		var firstName, lastName string
		var id, age int

		if err := rows.Scan(&id, &firstName, &lastName, &age); err != nil {
			log.Fatalln("Error reading row")
		}
		fmt.Printf("ID: %d, FirstName: %s, LastName: %s, Age: %d\n", id, firstName, lastName, age)

	}
	if err := rows.Close(); err != nil {
		log.Fatalln("Error closing rows")
	}
}
