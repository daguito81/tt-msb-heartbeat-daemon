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

// SbMsg is a struct to create a Service Bus Message
type SbMsg struct {
	ClientCode          string
	DeviceCode          string
	ReportFileProcessed string
	CurrentTime         string
	MsgRaw              bool
	MsgCode             string
	FamilyName          string
}

// db Database connection to be exported
var db *sql.DB

// ConnectDatabase connects to the database based on the environment variables
// DB_SERVER, DB_USER, DB_PASSWORD, DB_DATABASE
func ConnectDatabase() error {
	if err := godotenv.Load(); err != nil {
		return err
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
	var err error
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		return err
	}
	ctx := context.Background()

	if err := db.PingContext(ctx); err != nil {
		log.Error("Connect Database Ping Failed")
		return err
	}

	log.Info("Database Connected")

	return nil
}

// CountRowsOfDevice returns the number of rows present based on the parameters passed
func countRowsOfDevice(clientCode string, deviceCode string, msgCode string) (int, error) {
	log.Debug("Starting Counting Rows")
	ctx := context.Background()

	// Check if database is alive.
	log.Debug("Starting Ping Context")
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}
	log.Debug("Starting Query")
	q := fmt.Sprintf("SELECT COUNT(1) FROM device_test WHERE client_code = '%s' AND device_code = '%s' AND msg_code = '%s'",
		clientCode, deviceCode, msgCode)
	log.Debug("Executing Query")
	rows := db.QueryRowContext(ctx, q)
	log.Debug("Starting Scan")
	var total int
	if err := rows.Scan(&total); err != nil {
		log.Error("Error scanning Count Rows")
		return -1, err
	}
	return total, nil
}

// UpsertDevice will try to find the current device/client/msg combination
// If it already exists, it will update the lastPing time
// If it doesn't exist, it will insert a new row to DB
func UpsertDevice(msg SbMsg) error {
	log.Debug("Starting Upsert Device")
	t, err := countRowsOfDevice(msg.ClientCode, msg.DeviceCode, msg.MsgCode)
	if err != nil {
		log.Fatal("Error Counting rows")
		return err
	}

	if t > 0 {
		if err := updateDevice(msg.ClientCode, msg.DeviceCode, msg.MsgCode, msg.CurrentTime); err != nil {
			log.Error("Failed Update DB")
			return err
		}
	} else {
		if err := insertDevice(msg.ClientCode, msg.DeviceCode, msg.MsgCode, msg.CurrentTime); err != nil {
			log.Error("Failed Insert DB")
			return err
		}
	}
	return nil
}

func insertDevice(clientCode string, deviceCode string, msgCode string, lastPing string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return err
	}
	tsql := `
		INSERT INTO device_test (client_code, device_code, last_ping, msg_code)
		VALUES (@client_code, @device_code, @last_ping, @msg_code);
	`
	stmt, err := db.Prepare(tsql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	log.Info("Inserting New Device")
	_, err = stmt.ExecContext(
		ctx,
		sql.Named("client_code", clientCode),
		sql.Named("device_code", deviceCode),
		sql.Named("last_ping", lastPing),
		sql.Named("msg_code", msgCode),
	)
	if err != nil {
		log.Error("Error executing insert")
	}

	log.Infof("Row inserted - Client: %s Device: %s Msg: %s", clientCode, deviceCode, msgCode)
	return nil
}

func updateDevice(clientCode string, deviceCode string, msgCode string, lastPing string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return err
	}
	tsql := `
		UPDATE device_test
		SET last_ping = @last_ping
		WHERE client_code = @client_code
		AND device_code = @device_code
		AND msg_code = @msg_Code;
	`
	stmt, err := db.Prepare(tsql)
	if err != nil {
		log.Error("Error Preparing SQL Statement")
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(
		ctx,
		sql.Named("client_code", clientCode),
		sql.Named("device_code", deviceCode),
		sql.Named("msg_code", msgCode),
		sql.Named("last_ping", lastPing),
	)
	if err != nil {
		return err
	}

	return nil

}

// UpdateHeartbeat will update the current_heartbeat column every minute
func UpdateHeartbeat() error {
	log.Info("Updating Heartbeat")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return err
	}
	tsql := `
		UPDATE device_test
		SET current_heartbeat = GETDATE();
	`
	stmt, err := db.Prepare(tsql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		return err
	}

	return nil
}
