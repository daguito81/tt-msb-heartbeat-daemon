package main

import (
	"context"
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
	topicNameAPI  string = "api_topic"
	subName       string = "dagotest"
)

func main() {

	// FTP
	log.Info("Creating FTP Topic and Sub")
	_, err := sbmgmt.GetOrBuildTopic(topicNameFTP)
	if err != nil {
		log.Fatalf("Error getting FTP Topic: %v\n", err)
	}
	sftp, err := sbmgmt.GetOrBuildSubscription(subName, topicNameFTP)
	if err != nil {
		log.Fatalf("Error getting FTP Sub: %v\n", err)
	}

	// MQTT
	log.Info("Creating MQTT Topic and Sub")
	_, err = sbmgmt.GetOrBuildTopic(topicNameMQTT)
	if err != nil {
		log.Fatalf("Error getting MQTT Topic: %v\n", err)
	}
	smqtt, err := sbmgmt.GetOrBuildSubscription(subName, topicNameMQTT)
	if err != nil {
		log.Fatalf("Error getting MQTT Sub: %v\n", err)
	}

	// API
	log.Info("Creating API Topic and Sub")
	_, err = sbmgmt.GetOrBuildTopic(topicNameAPI)
	if err != nil {
		log.Fatalf("Error getting API Topic: %v\n", err)
	}
	sapi, err := sbmgmt.GetOrBuildSubscription(subName, topicNameAPI)
	if err != nil {
		log.Fatalf("Error getting API Sub: %v\n", err)
	}

	// Database
	log.Info("Creating Database Connection")
	if err := dbmgmt.ConnectDatabase(); err != nil {
		log.Fatalf("Error connecting to Database: %v\n", err)
	}

	// MAIN STUFF HERE
	ftpChan := make(chan bool)
	mqttChan := make(chan bool)
	apiChan := make(chan bool)
	go receiveMsgs(smqtt, mqttChan)
	go receiveMsgs(sftp, ftpChan)
	go receiveMsgs(sapi, apiChan)
	for {
		if err := dbmgmt.UpdateHeartbeat(); err != nil {
			log.Error(err)
			break
		}
		time.Sleep(5 * time.Second)
	}
	<-apiChan
	<-ftpChan
	<-mqttChan

}

func receiveMsgs(sub *servicebus.Subscription, c chan bool) {
	ctx := context.Background()

	// Change this from processFunc to printFunc
	var processMessage servicebus.HandlerFunc = processFunc

	log.Info("Starting to Receive Messages")
	if err := sub.Receive(ctx, processMessage); err != nil {
		log.Error("Error processing message:", err)
		c <- true
	}

}

func processFunc(ctx context.Context, msg *servicebus.Message) error {

	s := msg.Data
	var raw map[string]interface{}
	if err := json.Unmarshal(s, &raw); err != nil {
		return err
	}
	metaMap := raw["metadata"].(map[string]interface{})

	reportFileParsed, err := time.Parse("20060102150405", metaMap["reportfile_date"].(string))

	if err != nil {
		return err
	}
	reportFileResult := reportFileParsed.Format("2006-01-02 15:04:05")

	m := dbmgmt.SbMsg{
		ClientCode:          metaMap["client_code"].(string),
		DeviceCode:          metaMap["device_code"].(string),
		ReportFileProcessed: reportFileResult,
		CurrentTime:         time.Now().UTC().Format("2006-01-02 15:04:05"),
		MsgRaw:              metaMap["is_raw"].(bool),
		MsgCode:             metaMap["message_code"].(string),
	}

	if !m.MsgRaw {
		// fmt.Printf("%v/n", metaMap)
		fmt.Printf("Msg: ClientCode = %s, DeviceCode = %s, ReportFileDate: %s CurrentTime = %s, Raw: %v\n",
			m.ClientCode, m.DeviceCode, m.ReportFileProcessed, m.CurrentTime, m.MsgRaw)
		dbmgmt.UpsertDevice(m)
	}
	return msg.Complete(ctx)
}
