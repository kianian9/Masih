package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/kianian9/Masih/masih/broker"
	"strings"
	"sync"
	"time"
)

const (
	kafkaVersion   = "2.7.0"
	consumerOffset = sarama.OffsetOldest
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
	recv  chan []byte
}

// Peer stores specific Kafka broker connection information
type Peer struct {
	client        sarama.Client
	producer      sarama.AsyncProducer
	consumerGroup sarama.ConsumerGroup
	consumer      *Consumer
	context       context.Context
	send          chan []byte
	errors        chan error
	done          chan bool
	numMessages   uint
	messageSize   uint64
	brokersDown   chan bool
	topic         string
}

// BrokerPeer implements the peer interface for AMQP brokers
type BrokerPeer struct {
	connectionURL string
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
	topic         string
}

func (bp *BrokerPeer) SetupPublishers() error {
	if bp.producers > 0 {
		// Calculate nr messages to publish per publisher
		numMessPubArr := broker.DividePeerMessages(bp.producers, bp.numMessages)

		// Create publishers
		for i := 1; i <= bp.producers; i++ {
			publisherPeer := &Peer{
				send:        make(chan []byte),
				errors:      make(chan error, 1),
				done:        make(chan bool),
				numMessages: bp.numMessages,
				messageSize: bp.messageSize,
				brokersDown: make(chan bool),
				topic:       bp.topic,
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
			brokersDown: make(chan bool),
			topic:       bp.topic,
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

func (p *Peer) SetupPublishRoutine() {
	go func() {
		for {
			select {
			case msg := <-p.send:
				if err := p.sendMessage(msg); err != nil {
					p.errors <- err
				}
			case <-p.done:
				return
			}
		}
	}()
}

func (p *Peer) sendMessage(message []byte) error {
	select {
	case p.producer.Input() <- &sarama.ProducerMessage{Topic: p.topic, Key: nil, Value: sarama.ByteEncoder(message)}:
		return nil
	case err := <-p.producer.Errors():
		return err.Err
	}
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

func (p *Peer) ReceiveMessage() ([]byte, error) {
	// Await till the consumer has received message
	msg := <-p.consumer.recv
	return msg, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// NOTE: The function itself is called within a goroutine
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.recv <- message.Value
		session.MarkMessage(message, "")
	}
	return nil
}

func (p *Peer) SetupPublisherConnection(connectionURL string) error {
	var err error = nil
	// Connecting to the broker
	config := sarama.NewConfig()
	kfVersion, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return err
	}
	config.Version = kfVersion

	// Disabling compressing
	config.Producer.Compression = sarama.CompressionNone

	// Wait for partition leader to ack message
	config.Producer.RequiredAcks = sarama.WaitForLocal

	// Setting Round Robin partitioning for publishing messages
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	config.Net.DialTimeout = 2 * time.Second
	p.client, err = sarama.NewClient([]string{connectionURL}, config)
	if err != nil {
		return err
	}

	p.producer, err = sarama.NewAsyncProducer([]string{connectionURL}, config)
	if err != nil {
		return err
	}

	return nil
}

func (p *Peer) SetupSubscriberConnection(connectionURL string) error {
	var err error = nil
	// Connecting to the broker, with unique groupIds for receiving all records
	config := sarama.NewConfig()
	kfversion, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return err
	}
	config.Version = kfversion

	// Using the Round Robin partition balance strategy
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Consuming the earliest offset for the topic
	config.Consumer.Offsets.Initial = consumerOffset

	p.client, err = sarama.NewClient([]string{connectionURL}, config)
	if err != nil {
		return err
	}

	p.consumer = &Consumer{
		ready: make(chan bool),
		recv:  make(chan []byte),
	}

	p.consumerGroup, err = sarama.NewConsumerGroupFromClient(broker.GenerateName(), p.client)
	if err != nil {
		return err
	}
	p.context = context.Background()

	go func() {
		if err := p.consumerGroup.Consume(p.context, []string{p.topic}, p.consumer); err != nil {
			return
		}
	}()
	// Await till the consumer has been set up
	<-p.consumer.ready

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
		peer.client.Close()

	}
	// Closing subscriber sockets
	for _, element := range bp.subscriber {
		peer := element.PeerOperations.(*Peer)
		peer.consumerGroup.Close()
		peer.client.Close()
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
		topic:         settings.Topic,
	}
}
