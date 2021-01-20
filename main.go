package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	log "github.com/sirupsen/logrus"

	"github.com/daguito81/work/tt/main-service-bus-daemon/dbmgmt"
	"github.com/daguito81/work/tt/main-service-bus-daemon/sbmgmt"
)

const (
	topicNameFTP  string = "ftp_topic"
	topicNameMQTT string = "mqtt_topic"
	subName       string = "dagotest"
)

// var log = logrus.New()

func main() {
	// Set up Logging
	// log.Out = os.Stdout

	// FTP
	log.Info("Creating FTP Topic and Sub")
	tftp, err := sbmgmt.GetOrBuildTopic(topicNameFTP)
	if err != nil {
		log.Fatalf("Error getting FTP Topic: %v\n", err)
	}
	sftp, err := sbmgmt.GetOrBuildSubscription(subName, topicNameFTP)
	if err != nil {
		log.Fatalf("Error getting FTP Sub: %v\n", err)
	}

	// MQTT
	log.Info("Creating MQTT Topic and Sub")
	tmqtt, err := sbmgmt.GetOrBuildTopic(topicNameMQTT)
	if err != nil {
		log.Fatalf("Error getting MQTT Topic: %v\n", err)
	}
	smqtt, err := sbmgmt.GetOrBuildSubscription(subName, topicNameMQTT)
	if err != nil {
		log.Fatalf("Error getting MQTT Sub: %v\n", err)
	}
	_, _, _ = tftp, tmqtt, sftp
	// Database
	log.Info("Creating Database Connection")
	db, err := dbmgmt.ConnectDatabase()
	if err != nil {
		log.Fatalf("Error connecting to Database: %v\n", err)
	}
	_ = db
	receiveMsgs(smqtt)

}

func testDatabase(db *sql.DB) {
	// Test Query
	ctx := context.Background()
	tsql := fmt.Sprintf("SELECT * FROM test_table")
	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()

	for rows.Next() {
		var firstName, lastName string
		var id, age int

		if err := rows.Scan(&id, &firstName, &lastName, &age); err != nil {
			log.Fatalln("Error reading row")
		}
		fmt.Printf("ID: %d, FirstName: %s, LastName: %s, Age: %d\n", id, firstName, lastName, age)

	}
}

func receiveMsgs(sub *servicebus.Subscription) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Change this from processFunc to printFunc
	var processMessage servicebus.HandlerFunc = processFunc

	log.Info("Starting to Receive Messages")
	if err := sub.Receive(ctx, processMessage); err != nil {
		log.Error("Error processing message:", err)
	}

}

func processFunc(ctx context.Context, msg *servicebus.Message) error {
	type sbMsg struct {
		clientCode          string
		deviceCode          string
		reportFileProcessed string
		currentTime         string
		msgRaw              bool
	}

	s := msg.Data
	var raw map[string]interface{}
	if err := json.Unmarshal(s, &raw); err != nil {
		return err
	}
	metaMap := raw["metadata"].(map[string]interface{})

	reportFileParsed, err := time.Parse("20060102150405", string(metaMap["reportfile_date"].(string)))

	if err != nil {
		return err
	}
	reportFileResult := reportFileParsed.Format("2006-01-02 15:04:05")

	m := sbMsg{
		clientCode:          metaMap["client_code"].(string),
		deviceCode:          metaMap["device_code"].(string),
		reportFileProcessed: reportFileResult,
		currentTime:         time.Now().UTC().Format("2006-01-02 15:04:05"),
		msgRaw:              metaMap["is_raw"].(bool),
	}

	if !m.msgRaw {
		fmt.Printf("Msg: ClientCode = %s, DeviceCode = %s, ReportFileDate: %s CurrentTime = %s, Raw: %v\n",
			m.clientCode, m.deviceCode, m.reportFileProcessed, m.currentTime, m.msgRaw)
	}
	return msg.Complete(ctx)
}

// func updateDB(clientCode string, deviceCode string)
