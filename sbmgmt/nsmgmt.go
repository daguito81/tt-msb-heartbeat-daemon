package sbmgmt

import (
	"os"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/joho/godotenv"
)

// GetServiceBusNamespace finds the ASB namespace and returns it
func GetServiceBusNamespace() (*servicebus.Namespace, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}
	connStr := os.Getenv("SERVICE_BUS_CONN_STR")

	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		return nil, err
	}
	return ns, nil
}
