package sbmgmt

import (
	"context"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	log "github.com/sirupsen/logrus"
)

// GetOrBuildTopic creates a topic and returns the client or error
func GetOrBuildTopic(topicName string) (*servicebus.Topic, error) {

	ns, err := GetServiceBusNamespace()
	if err != nil {
		log.Fatalln("Error connecting to Service Bus: ", err)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a new Queue Manager
	tm := ns.NewTopicManager()
	te, err := tm.Get(ctx, topicName)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return nil, err
	}

	if te == nil {
		_, err := tm.Put(ctx, topicName)
		if err != nil {
			return nil, err
		}
	}

	t, err := ns.NewTopic(topicName)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// DeleteTopic deletes the named queue from Service Bus
func DeleteTopic(topicName string) error {
	ns, err := GetServiceBusNamespace()
	if err != nil {
		log.Fatalln("Error connecting to Service Bus: ", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Delete Queue
	tm := ns.NewTopicManager()
	if err = tm.Delete(ctx, topicName); err != nil {
		log.Fatalln("Error deleting Queue: ", err)
		return err
	}
	return nil

}

// GetOrBuildSubscription creates a new subscription or gets an existint subscription
func GetOrBuildSubscription(subName string, topicName string) (*servicebus.Subscription, error) {
	ns, err := GetServiceBusNamespace()
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a new Topic Manager
	sm, err := ns.NewSubscriptionManager(topicName)
	if err != nil {
		log.Fatalln(err)
	}
	se, err := sm.Get(ctx, subName)
	if err != nil && !servicebus.IsErrNotFound(err) {
		log.Errorf("ERROR: Getting the current subscription: %s", subName)
		return nil, err
	}
	// In case of empty, create subscription
	var lockDuration = time.Minute
	var msgDuration = 12 * time.Hour
	if se == nil {
		log.Info("Creating New Subscription")
		_, err := sm.Put(ctx, subName,
			servicebus.SubscriptionWithMessageTimeToLive(&msgDuration),
			servicebus.SubscriptionWithLockDuration(&lockDuration))
		if err != nil {
			log.Error("ERROR: Creating New subscription")
			return nil, err
		}
	}
	// Create sub client
	s, err := sm.Topic.NewSubscription(subName)
	if err != nil {
		log.Error("Error creating subscription client")
		return nil, err
	}
	return s, nil
}
