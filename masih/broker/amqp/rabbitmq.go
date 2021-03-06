package amqp

import (
	"fmt"
	"github.com/kianian9/Masih/masih/broker"
	"github.com/streadway/amqp"
	"strconv"
	"strings"
	"sync"
)

const (
	exchange               = "logs"
	exchangeType           = "fanout"
	QUOROM_QUEUE           = "QUORUM"
	CLASSIC_QUEUE          = "CLASSIC"
	MAX_QUEUE_LENGTH_BYTES = 1024 * 1024 * 20 // 20MB
)

// Peer stores specific AMQP broker connection information
type Peer struct {
	conn        *amqp.Connection
	queue       *amqp.Queue
	channel     *amqp.Channel
	inbound     <-chan amqp.Delivery
	send        chan []byte
	errors      chan error
	done        chan bool
	consumerTag string
}

// BrokerPeer implements the peer interface for AMQP brokers
type BrokerPeer struct {
	connectionURL                  string
	queueType                      string
	consumers                      int
	producers                      int
	messageSize                    uint64
	numMessages                    uint
	publisher                      []*broker.Publisher
	subscriber                     []*broker.Subscriber
	syncMutex                      *sync.Mutex
	syncCond                       *sync.Cond
	nrReadyPeers                   *int
	nrDonePeers                    *int
	subscriberNrConsumedMessageArr []*uint
	subscribersDoneArr             []*bool
}

func (bp *BrokerPeer) SetupPublishers() error {
	if bp.producers > 0 {
		// Calculate nr messages to publish per publisher
		numMessPubArr := broker.DividePeerMessages(bp.producers, bp.numMessages)

		// Create publishers
		for i := 1; i <= bp.producers; i++ {
			publisherPeer := &Peer{
				send:   make(chan []byte),
				errors: make(chan error, 1),
				done:   make(chan bool),
			}
			publisher := &broker.Publisher{
				PeerOperations:           publisherPeer,
				Id:                       i,
				NrMessagesToPublish:      numMessPubArr[i-1],
				MessageSize:              bp.messageSize,
				SyncMutex:                bp.syncMutex,
				SyncCond:                 bp.syncCond,
				NrDonePeers:              bp.nrDonePeers,
				NrReadyPeers:             bp.nrReadyPeers,
				SubNrConsumedMessagesArr: bp.subscriberNrConsumedMessageArr,
				SubscriberDoneArr:        bp.subscribersDoneArr,
				NrPublishers:             bp.producers,
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
	if bp.consumers > 0 {
		// Calculate nr messages to consumer per subscriber
		numMessSubArr := broker.DividePeerMessages(bp.consumers, bp.numMessages)

		// Create subscribers
		for i := 1; i <= bp.consumers; i++ {
			subscriberPeer := &Peer{
				consumerTag: "subscriber" + strconv.Itoa(i),
			}
			subscriber := &broker.Subscriber{
				PeerOperations:      subscriberPeer,
				Id:                  i,
				NrMessagesToConsume: numMessSubArr[i-1],
				HasStarted:          false,
				Started:             0,
				Stopped:             0,
				SyncMutex:           bp.syncMutex,
				SyncCond:            bp.syncCond,
				NrDonePeers:         bp.nrDonePeers,
				NrReadyPeers:        bp.nrReadyPeers,
				NrMessagesConsumed:  bp.subscriberNrConsumedMessageArr[i-1],
				SubscriberDone:      bp.subscribersDoneArr[i-1],
			}
			bp.subscriber[i-1] = subscriber

			// Setup subscriber connection
			err := subscriberPeer.SetupSubscriberConnection(bp.connectionURL, bp.queueType)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Goroutine which fetch messages from send-channel and publish them
func (p *Peer) SetupPublishRoutine() {
	go func() {
		for {
			select {
			case msg := <-p.send:
				if err := p.channel.Publish(
					exchange, // exchange
					"",       // routing key
					false,    // mandatory
					false,    // immediate
					amqp.Publishing{
						Body:         msg,
						DeliveryMode: 2,
					},
				); err != nil {
					p.errors <- err
				}
			case <-p.done:
				return
			}
		}
	}()
}

// Send returns a channel on which messages can be sent for publishing.
func (p *Peer) SendChannel() chan<- []byte {
	return p.send
}

// Errors returns the channel on which the peer sends publish errors.
func (p *Peer) ErrorChannel() <-chan error {
	return p.errors
}

// Done signals to the peer that message publishing has completed.
func (p *Peer) DoneChannel() {
	p.done <- true
}

func (p *Peer) ReceiveMessage() ([]byte, error) {
	message := <-p.inbound
	return message.Body, nil
}

func (p *Peer) SetupPublisherConnection(connectionURL string) error {
	var err error = nil
	// Connecting to the broker
	p.conn, err = amqp.Dial(connectionURL)
	if err != nil {
		return err
	}

	p.channel, err = p.conn.Channel()
	if err != nil {
		return err
	}

	// Sets the channel's exchange
	err = p.channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return err
	}

	return nil
}

func (p *Peer) SetupSubscriberConnection(connectionURL, queueType string) error {
	var err error = nil
	// Connecting to the broker
	p.conn, err = amqp.Dial(connectionURL)
	if err != nil {
		return err
	}

	// Creates a channel by the connection
	p.channel, err = p.conn.Channel()
	if err != nil {
		return err
	}

	// Sets the channel's exchange
	err = p.channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return err
	}

	args := make(amqp.Table)
	// If not quorum queue, it will by default create a classic mirrored queue
	// Both queue types will only use disk for storing messages
	if strings.EqualFold(QUOROM_QUEUE, queueType) {
		args["x-queue-type"] = "quorum"
		args["x-max-in-memory-length"] = 0
	} else {
		args["x-queue-mode"] = "lazy"
	}

	// Setting queue max-length in bytes for not overflowing queue
	// Especially important when subscribers are done and publishers still publishing messages
	// NOTE: This value must be larger than maxBytesBehind in publisher!
	args["x-max-length-bytes"] = MAX_QUEUE_LENGTH_BYTES

	// Declaring a durable queue
	q, err := p.channel.QueueDeclare(
		broker.GenerateName(), // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		args,                  // arguments
	)
	if err != nil {
		return err
	}
	p.queue = &q

	// Binding the queue to the exchange
	err = p.channel.QueueBind(
		q.Name,   // queue name
		"",       // routing key
		exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		return err
	}

	p.inbound, err = p.channel.Consume(
		p.queue.Name,  // queue
		p.consumerTag, // consumerTag
		true,          // auto-ack
		false,         // exclusive
		true,          // no-local
		false,         // no-wait
		nil,           // args
	)

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
		element := element
		go func() {
			element.StartSubscribing(nrPeers)
			peer := element.PeerOperations.(*Peer)
			// Disconnecting subscriber - Will otherwise continue receive data
			peer.channel.Close()
			peer.conn.Close()
		}()
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
		peer.channel.Close()
		peer.conn.Close()
	}
	// Closing subscriber sockets
	for _, element := range bp.subscriber {
		peer := element.PeerOperations.(*Peer)
		peer.channel.Close()
		peer.conn.Close()
	}
}

// NewBrokerPeer creates and returns a new Peer for communicating with AMQP
func NewBrokerPeer(settings broker.MQSettings) *BrokerPeer {
	connectionURL := "amqp://" + settings.Username + ":" + settings.Password + "@" +
		settings.BrokerHost + ":" + settings.BrokerPort + "/"
	m := sync.Mutex{}
	c := sync.NewCond(&m)
	nrReadyPeers := new(int)
	nrDonePeers := new(int)
	*nrDonePeers = 0
	*nrReadyPeers = 0

	subscriberMessRead := make([]*uint, settings.Consumers)
	subscribersDone := make([]*bool, settings.Consumers)
	queuesUnsubscribed := make([]*bool, settings.Consumers)

	for i := 0; i < int(settings.Consumers); i++ {
		messageRead := new(uint)
		*messageRead = 0
		subscriberMessRead[i] = messageRead
		subscriberDone := new(bool)
		*subscriberDone = false
		queueUnsubscribed := new(bool)
		*queueUnsubscribed = false
		subscribersDone[i] = subscriberDone
		queuesUnsubscribed[i] = queueUnsubscribed
	}

	return &BrokerPeer{
		connectionURL:                  connectionURL,
		queueType:                      settings.QueueType,
		consumers:                      int(settings.Consumers),
		producers:                      int(settings.Producers),
		messageSize:                    settings.MessageSize,
		numMessages:                    settings.NumMessages,
		publisher:                      make([]*broker.Publisher, settings.Producers),
		subscriber:                     make([]*broker.Subscriber, settings.Consumers),
		syncMutex:                      &m,
		syncCond:                       c,
		nrReadyPeers:                   nrReadyPeers,
		nrDonePeers:                    nrDonePeers,
		subscriberNrConsumedMessageArr: subscriberMessRead,
		subscribersDoneArr:             subscribersDone,
	}
}
