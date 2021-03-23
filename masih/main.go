package main

import (
	"flag"
	"fmt"
	"github.com/kianian9/Masih/masih/broker"
	"github.com/kianian9/Masih/masih/broker/amqp"
	"github.com/kianian9/Masih/masih/broker/kafka"
	"github.com/kianian9/Masih/masih/broker/stan"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type peer interface {
	// Setups publishers/subscribers connections
	SetupPublishers() error
	SetupSubscribers() error

	// Starts publishers to a defined topic (set by broker)
	StartPublishers()

	// Starts subscribers to a defined topic (set by broker)
	StartSubscribers()

	GetResults() *broker.Results

	// Performs any broker-connection cleanups after test is done
	Teardown()
}

const (
	defaultBroker       = "rabbitmq"
	defaultHost         = "localhost"
	defaultBrokerPort   = "5672"
	defaultUsername     = "guest"
	defaultPassword     = "guest"
	defaultMessageSize  = 1000
	defaultNumMessages  = 500000
	defaultNumProducers = 1
	defaultNumConsumers = 1
	defaultQueueType    = "QUORUM"
	defaultClusterID    = "stan"
)

// The supported message brokers
const (
	RabbitMQ = "rabbitmq"
	Kafka    = "kafka"
	STAN     = "stan"
)

var brokers = []string{
	"rabbitmq",
	"kafka",
	"stan",
}

func main() {
	var (
		brokerName  = flag.String("broker", defaultBroker, brokerList())
		brokerHost  = flag.String("host", defaultHost, "broker's host address")
		brokerPort  = flag.String("port", defaultBrokerPort, "broker port")
		username    = flag.String("username", defaultUsername, "username for amqp brokers")
		password    = flag.String("password", defaultPassword, "password for amqp brokers")
		messageSize = flag.Uint64("messageSize", defaultMessageSize, "size of each message in bytes")
		numMessages = flag.Uint("numMessages", defaultNumMessages, "total number of messages to send")
		producers   = flag.Uint("producers", defaultNumProducers, "number of producers per host")
		consumers   = flag.Uint("consumers", defaultNumConsumers, "number of consumers per host")
		queueType   = flag.String("queueType", defaultQueueType, "queue type for amqp brokers")
		clusterID   = flag.String("clusterID", defaultClusterID, "cluster id for nats streaming brokers")
	)

	flag.Parse()

	// Checking if given broker is one of the supported ones
	if !strings.Contains(brokerList(), *brokerName) {
		fmt.Fprintf(os.Stderr, "Error: The provided broker is not supported!\n")
		os.Exit(1)
	}

	// Checking if the broker's port is a valid or not
	if _, err := strconv.Atoi(*brokerPort); err != nil {
		fmt.Fprintf(os.Stderr, "Error: The provided port %q is not a valid port!\n", *brokerPort)
		os.Exit(1)
	}

	if *producers == 0 && *consumers == 0 {
		fmt.Fprintf(os.Stderr, "Error: There must be at least one producer or consumer!\n")
		os.Exit(1)
	}

	brokerSetting := broker.MQSettings{
		BrokerName:  *brokerName,
		BrokerHost:  *brokerHost,
		BrokerPort:  *brokerPort,
		Username:    *username,
		Password:    *password,
		MessageSize: *messageSize,
		NumMessages: *numMessages,
		Producers:   *producers,
		Consumers:   *consumers,
		QueueType:   *queueType,
		ClusterID:   *clusterID,
	}

	fmt.Println("BrokerName: ", brokerSetting.BrokerName)
	fmt.Println("Host: ", brokerSetting.BrokerHost)
	fmt.Println("BrokerPort: ", brokerSetting.BrokerPort)
	fmt.Println("Username: ", brokerSetting.Username)
	fmt.Println("Password: ", brokerSetting.Password)
	fmt.Println("MessageSize: ", brokerSetting.MessageSize)
	fmt.Println("Num Messages: ", brokerSetting.NumMessages)
	fmt.Println("Nr Producers: ", brokerSetting.Producers)
	fmt.Println("Nr Consumers: ", brokerSetting.Consumers)
	fmt.Println("Queue type: ", brokerSetting.QueueType)

	runtime.GOMAXPROCS(runtime.NumCPU())

	// Setups broker data needed for connecting
	brokerPeer := newBrokerPeer(brokerSetting)
	if brokerPeer == nil {
		fmt.Fprintf(os.Stderr, "Error: Invalid broker: %s\n", *brokerName)
		os.Exit(1)
	}

	// Creates publishers and subscribers for the broker
	err := brokerPeer.SetupPublishers()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}

	err = brokerPeer.SetupSubscribers()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}

	startTime := time.Now()

	// Starts publishers to mq
	brokerPeer.StartPublishers()

	// Starts subscribers to mq
	brokerPeer.StartSubscribers()

	// Waits and gets the peer results
	peerResults := brokerPeer.GetResults()

	elapsedTime := time.Since(startTime)

	// Prints summary of test
	printSummary(brokerSetting, elapsedTime)

	// Prints the results
	peerResults.PrintResults()

	// Closing all sockets/cleaning up
	brokerPeer.Teardown()

}

func printSummary(settings broker.MQSettings, elapsed time.Duration) {
	msgSent := int(settings.NumMessages)
	msgRecv := int(settings.NumMessages) * int(settings.Consumers)
	dataSentKB := (msgSent * int(settings.MessageSize)) / 1000
	dataRecvKB := (msgRecv * int(settings.MessageSize)) / 1000
	fmt.Printf("\nTEST SUMMARY\n")
	fmt.Printf("Time Elapsed:       	     %s\n", elapsed.String())
	fmt.Printf("Broker:             	     %s (%s)\n", settings.BrokerName, settings.BrokerHost)
	fmt.Printf("Number Publishers:  	     %d\n", settings.Producers)
	fmt.Printf("Number Subscribers: 	     %d\n", settings.Consumers)
	fmt.Printf("Total messages produced:  	 %d\n", msgSent)
	fmt.Printf("Total messages consumed:  	 %d\n", msgRecv)
	fmt.Printf("Bytes per message:  	     %d\n", settings.MessageSize)
	fmt.Printf("Total data produced (KB): 	 %d\n", dataSentKB)
	fmt.Printf("Total data consumed (KB): 	 %d\n", dataRecvKB)
	fmt.Println("")
}

func brokerList() string {
	brokerList := "["
	for i, b := range brokers {
		brokerList = brokerList + b
		if i != len(brokers)-1 {
			brokerList = brokerList + "|"
		}
	}
	brokerList = brokerList + "]"
	return brokerList
}

func newBrokerPeer(brokerSettings broker.MQSettings) peer {

	switch brokerSettings.BrokerName {
	case RabbitMQ:
		return amqp.NewBrokerPeer(brokerSettings)
	case Kafka:
		return kafka.NewBrokerPeer(brokerSettings)
	case STAN:
		return stan.NewBrokerPeer(brokerSettings)
	default:
		return nil
	}
}
