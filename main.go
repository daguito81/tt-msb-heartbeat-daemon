package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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
	nw            int    = 5
)

func main() {

	// Async Setup
	clientsFTP := make(chan *servicebus.Subscription, nw)
	clientsMQTT := make(chan *servicebus.Subscription, nw)
	clientsAPI := make(chan *servicebus.Subscription, nw)

	var wgFTP, wgAPI, wgMQTT sync.WaitGroup

	// FTP
	createSBClient("FTP", topicNameFTP, &wgFTP, clientsFTP)
	wgFTP.Wait()

	// MQTT
	createSBClient("MQTT", topicNameMQTT, &wgMQTT, clientsMQTT)
	wgMQTT.Wait()

	// API
	createSBClient("API", topicNameAPI, &wgAPI, clientsAPI)
	wgAPI.Wait()

	// Database
	log.Info("Creating Database Connection")
	if err := dbmgmt.ConnectDatabase(); err != nil {
		log.Fatalf("Error connecting to Database: %v\n", err)
	}

	// MAIN STUFF HERE

	for i := 0; i < nw; i++ {
		clientFTP := <-clientsFTP
		clientMQTT := <-clientsMQTT
		clientAPI := <-clientsAPI
		go receiveMsgs(clientFTP, "FTP")
		go receiveMsgs(clientMQTT, "MQTT")
		go receiveMsgs(clientAPI, "API")
	}
	close(clientsFTP)
	close(clientsMQTT)
	close(clientsAPI)
	for {
		if err := dbmgmt.UpdateHeartbeat(); err != nil {
			log.Error(err)
			break
		}
		time.Sleep(30 * time.Second)
	}

}

func createSBClient(f string, topicName string, wg *sync.WaitGroup, c chan *servicebus.Subscription) {
	log.Infof("Creating %s Sub and Clients", f)
	for i := 0; i < nw; i++ {
		log.Infof("Creating Client for %s", f)
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			s, err := sbmgmt.GetOrBuildSubscription(subName, topicName)
			if err != nil {
				log.Fatalf("Error getting %s Sub: %v\n", f, err)
			}
			log.Infof("Created %s Sub Client: %d", f, i)
			c <- s
			wg.Done()

		}(i, wg)
	}
}

func receiveMsgs(sub *servicebus.Subscription, f string) {
	ctx := context.Background()

	var processMessage servicebus.HandlerFunc = processFunc

	log.Infof("Starting to Receive Messages for %s", f)
	if err := sub.Receive(ctx, processMessage); err != nil {
		log.Error("Error processing message:", err)
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
		FamilyName:          metaMap["family_name"].(string),
	}

	if !m.MsgRaw {
		// fmt.Printf("%v/n", metaMap)
		fmt.Printf("Msg: ClientCode = %s\t DeviceCode = %s\t FamilyName = %s\t Raw: %v\n",
			m.ClientCode, m.DeviceCode, m.FamilyName, m.MsgRaw)
		if err := dbmgmt.UpsertDevice(m); err != nil {
			log.Error("Msg Processing Error! ", err)
			return err

		}
	}
	return msg.Complete(ctx)
}
