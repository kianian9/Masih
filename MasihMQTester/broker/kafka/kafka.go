package kafka

import (
	"Masih/MasihMQTester/broker"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Peer stores specific AMQP broker connection information
type Peer struct {
	producer        *kafka.Producer
	consumer        *kafka.Consumer
	send            chan []byte
	errors          chan error
	done            chan bool
	numMessages     uint
	messageSize     uint64
	messagesFlushed chan bool
}

// BrokerPeer implements the peer interface for AMQP brokers
type BrokerPeer struct {
	connectionURL string
	queueType     string
	consumers     int
	producers     int
	messageSize   uint64
	numMessages   uint
	publisher     []*broker.Publisher
	subscriber    []*broker.Subscriber
	syncMutex     *sync.Mutex
	syncCond      *sync.Cond
	nrReadyPeers  *int
	nrDonePeers   *int
}

func (bp *BrokerPeer) SetupPublishers() error {

	if bp.producers > 0 {
		// Calculate nr messages to publish per publisher
		numMessPubArr := broker.DividePeerMessages(bp.producers, bp.numMessages)

		// Create publishers
		for i := 1; i <= bp.producers; i++ {
			publisherPeer := &Peer{
				producer:        nil,
				consumer:        nil,
				send:            make(chan []byte),
				errors:          make(chan error, 1),
				done:            make(chan bool),
				numMessages:     bp.numMessages,
				messageSize:     bp.messageSize,
				messagesFlushed: make(chan bool),
			}
			publisher := &broker.Publisher{
				PeerOperations:      publisherPeer,
				Id:                  i,
				NrMessagesToPublish: numMessPubArr[i-1],
				MessageSize:         bp.messageSize,
				SyncMutex:           bp.syncMutex,
				SyncCond:            bp.syncCond,
				NrDonePeers:         bp.nrDonePeers,
				NrReadyPeers:        bp.nrReadyPeers,
			}
			bp.publisher[i-1] = publisher

			// Setup publisher connection
			err := publisherPeer.SetupPublisherConnection(bp.connectionURL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bp *BrokerPeer) SetupSubscribers() error {
	subCounter := new(uint)
	*subCounter = 0

	// Create subscribers
	for i := 1; i <= bp.consumers; i++ {
		subscriberPeer := &Peer{
			producer: nil,
			consumer: nil,
			send:     nil,
			errors:   nil,
			done:     nil,
		}
		subscriber := &broker.Subscriber{
			PeerOperations: subscriberPeer,
			Id:             i,
			NumMessages:    bp.numMessages,
			HasStarted:     false,
			Started:        0,
			Stopped:        0,
			SyncMutex:      bp.syncMutex,
			SyncCond:       bp.syncCond,
			NrDonePeers:    bp.nrDonePeers,
			NrReadyPeers:   bp.nrReadyPeers,
		}
		bp.subscriber[i-1] = subscriber

		// Setup subscriber connection
		err := subscriberPeer.SetupSubscriberConnection(bp.connectionURL)
		if err != nil {
			return err
		}

	}
	return nil
}

func (p *Peer) HandleErrors() {
	// Delivery report handler for produced messages
	go func() {
		for {
			for e := range p.producer.Events() {
				switch ev := e.(type) {

				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						p.errors <- ev.TopicPartition.Error
					}
				case kafka.Error:
					if ev.Code() == kafka.ErrAllBrokersDown {
						p.errors <- ev
					}
				}

			}
			if <-p.done {
				return
			}
		}

	}()
}

// Goroutine which fetch messages from send-channel and publish them
func (p *Peer) SetupPublishRoutine() {
	//p.HandleErrors()
	topic := broker.Topic
	go func() {
		counter := 0
		for {
			select {
			case msg := <-p.send:
				p.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          msg,
				}, nil)
				counter++
			case <-p.done:
				// Waiting up to 10 minutes until all messages are successfully delivered
				p.producer.Flush(600 * 1000)
				p.messagesFlushed <- true
				return
			}
		}
	}()
}

// Send returns a channel on which messages can be sent for publishing.
func (p *Peer) SendChannel() chan<- []byte {
	return p.send
}

// ErrorChannel returns the channel on which the peer sends publish errors.
func (p *Peer) ErrorChannel() <-chan error {
	return p.errors
}

// DoneChannel signals to the peer that message publishing has completed.
func (p *Peer) DoneChannel() {
	p.done <- true
}

// DeliveredChannel returns the channel on which the peer can check for delivery completion.
func (p *Peer) DeliveredChannel() <-chan bool {
	return p.messagesFlushed
}

func (p *Peer) ReceiveMessage() ([]byte, error) {
	msg, err := p.consumer.ReadMessage(10 * time.Second)
	if msg != nil && msg.Value != nil {
		return msg.Value, err
	}
	return nil, err
}

// TODO: Fix that publisher creates topic before subscriber trying to subcribe to it

func (p *Peer) SetupPublisherConnection(connectionURL string) error {
	// Connecting to the broker
	nrMaxBufferedMsgs := strconv.FormatUint(uint64(p.numMessages), 10)
	maxBufferSizeInKB := strconv.FormatUint((uint64(p.numMessages)*p.messageSize)/1000, 10)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            connectionURL,
		"acks":                         "all",
		"queue.buffering.max.messages": nrMaxBufferedMsgs,
		"queue.buffering.max.kbytes":   maxBufferSizeInKB,
	})

	if err != nil {
		return err
	}
	p.producer = producer

	p.HandleErrors()

	// Small wait for possible broker error
	time.Sleep(500 * time.Microsecond)
	if len(p.errors) != 0 {
		if err = <-p.errors; err != nil {
			return err
		}
	}

	return nil
}

func (p *Peer) SetupSubscriberConnection(connectionURL string) error {
	// Connecting to the broker, with unique groupIds for receiving all records
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":             connectionURL,
		"group.id":                      broker.GenerateName(),
		"enable.auto.commit":            "true",
		"auto.commit.interval.ms":       "100",
		"auto.offset.reset":             "earliest",
		"partition.assignment.strategy": "roundrobin",
	})
	if err != nil {
		return err
	}
	p.consumer = consumer

	err = consumer.SubscribeTopics([]string{broker.Topic}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (bp *BrokerPeer) StartPublishers() {
	nrPeers := bp.producers + bp.consumers
	for _, element := range bp.publisher {
		go element.StartPublishing(nrPeers)
	}
}

func (bp *BrokerPeer) StartSubscribers() {
	nrPeers := bp.producers + bp.consumers
	for _, element := range bp.subscriber {
		go element.StartSubscribing(nrPeers)
	}
}

func (bp *BrokerPeer) GetResults() *broker.Results {
	// Wait for all peers to be done
	totalNrPeers := bp.consumers + bp.producers
	fmt.Printf("\nWaiting for peers to be done\n\n")
	bp.syncMutex.Lock()
	for totalNrPeers != *bp.nrDonePeers {
		bp.syncCond.Wait()
	}
	fmt.Println("All peers done, getting results...")
	bp.syncMutex.Unlock()

	publisherResults := make([]*broker.Result, bp.producers)
	for i, element := range bp.publisher {
		publisherResults[i] = element.Results
	}
	subscriberResults := make([]*broker.Result, bp.consumers)
	for i, element := range bp.subscriber {
		subscriberResults[i] = element.Results
	}

	return &broker.Results{
		PublisherResults:  publisherResults,
		SubscriberResults: subscriberResults,
	}

}

// Performs any broker-connection cleanup after test is done
func (bp *BrokerPeer) Teardown() {
	// Closing publisher sockets
	for _, element := range bp.publisher {
		peer := element.PeerOperations.(*Peer)
		peer.producer.Close()

	}
	// Closing subscriber sockets
	for _, element := range bp.subscriber {
		peer := element.PeerOperations.(*Peer)
		err := peer.consumer.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Subscriber %d failed to close socket\n", element.Id)
		}
	}

}

// NewPeer creates and returns a new Peer for communicating with Kafka
func NewBrokerPeer(settings broker.MQSettings) *BrokerPeer {
	connectionURL := strings.Split(settings.BrokerHost, ":")[0] + ":" + settings.BrokerPort

	m := sync.Mutex{}
	c := sync.NewCond(&m)
	nrReadyPeers := new(int)
	nrDonePeers := new(int)
	*nrDonePeers = 0
	*nrReadyPeers = 0

	return &BrokerPeer{
		connectionURL: connectionURL,
		queueType:     settings.QueueType,
		consumers:     int(settings.Consumers),
		producers:     int(settings.Producers),
		messageSize:   settings.MessageSize,
		numMessages:   settings.NumMessages,
		publisher:     make([]*broker.Publisher, settings.Producers),
		subscriber:    make([]*broker.Subscriber, settings.Consumers),
		syncMutex:     &m,
		syncCond:      c,
		nrReadyPeers:  nrReadyPeers,
		nrDonePeers:   nrDonePeers,
	}
}
