package stan

import (
	"fmt"
	"github.com/kianian9/Masih/masih/broker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Peer stores specific NATS/STAN broker connection information
type Peer struct {
	id              int
	clusterID       string
	natsConnection  *nats.Conn
	stanConnection  stan.Conn
	ackHandler      func(ackedNuid string, err error)
	send            chan []byte
	subscription    stan.Subscription
	recv            chan []byte
	errors          chan error
	done            chan bool
	numMessages     uint
	messageSize     uint64
	messagesFlushed chan bool
}

// BrokerPeer implements the peer interface for AMQP brokers
type BrokerPeer struct {
	connectionURL string
	clusterID     string
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
				id:              i,
				clusterID:       bp.clusterID,
				natsConnection:  nil,
				stanConnection:  nil,
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
			id:             i,
			clusterID:      bp.clusterID,
			natsConnection: nil,
			stanConnection: nil,
			recv:           make(chan []byte),
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

// ACKhandler for produced messages
func (p *Peer) ACKHandler() {
	p.ackHandler = func(ackedNuid string, err error) {
		if err != nil {
			p.errors <- err
		}
	}
}

// Goroutine which fetch messages from send-channel and publish them
func (p *Peer) SetupPublishRoutine() {
	go func() {
		for {
			select {
			case msg := <-p.send:
				_, err := p.stanConnection.PublishAsync(broker.Topic, msg, p.ackHandler) // returns immediately
				if err != nil {
					p.errors <- err
				}

			case <-p.done:
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
	message := <-p.recv
	return message, nil
}

func (p *Peer) SetupPublisherConnection(connectionURL string) error {
	var err error = nil
	// Connecting to the NATS broker
	p.natsConnection, err = nats.Connect(connectionURL)
	if err != nil {
		return err
	}

	// Setting streaming connection and its preferences
	clientID := "producer" + strconv.Itoa(p.id)
	p.stanConnection, err = stan.Connect(p.clusterID, clientID,
		stan.NatsConn(p.natsConnection), stan.Pings(10, 5),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			fmt.Fprintf(os.Stderr, "Connection lost, reason: %v", reason)
		}))
	if err != nil {
		return err
	}

	return nil
}

func (p *Peer) SetupSubscriberConnection(connectionURL string) error {
	var err error = nil
	// Connecting to the broker
	p.natsConnection, err = nats.Connect(connectionURL)
	if err != nil {
		return err
	}

	// Setting streaming connection and its preferences
	clientID := "subscriber" + strconv.Itoa(p.id)
	p.stanConnection, err = stan.Connect(p.clusterID, clientID,
		stan.NatsConn(p.natsConnection), stan.Pings(10, 5),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			fmt.Fprintf(os.Stderr, "Connection lost, reason: %v", reason)
		}))
	if err != nil {
		return err
	}

	p.subscription, err = p.stanConnection.Subscribe(broker.Topic, func(m *stan.Msg) {
		p.recv <- m.Data
	})
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
		peer.stanConnection.Close()
	}
	// Closing subscriber sockets
	for _, element := range bp.subscriber {
		peer := element.PeerOperations.(*Peer)
		peer.subscription.Unsubscribe()
		peer.stanConnection.Close()
	}

}

// NewPeer creates and returns a new Peer for communicating with Kafka
func NewBrokerPeer(settings broker.MQSettings) *BrokerPeer {
	connectionURL := "nats://" + strings.Split(settings.BrokerHost, ":")[0] + ":" + settings.BrokerPort
	m := sync.Mutex{}
	c := sync.NewCond(&m)
	nrReadyPeers := new(int)
	nrDonePeers := new(int)
	*nrDonePeers = 0
	*nrReadyPeers = 0

	return &BrokerPeer{
		connectionURL: connectionURL,
		clusterID:     settings.ClusterID,
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
