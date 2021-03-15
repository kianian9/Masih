package kafka

// Peer implements the peer interface for Kafka
/*
type Peer struct {
	client   sarama.Client
	producer sarama.AsyncProducer
	consumer sarama.PartitionConsumer
	send     chan []byte
	errors   chan error
	done     chan bool
}
*/

type Peer struct {
}

// Initializes the broker connection
func (p *Peer) Setup() {

}

// Starts one publisher to a defined topic (set by broker)
func (p *Peer) Publish() error {

	return nil
}

// Starts one subscriber to a defined topic (set by broker)
func (p *Peer) Subscribe() error {

	return nil
}

// Performs any broker-connection cleanup after test is done
func (p *Peer) Teardown() {

}
