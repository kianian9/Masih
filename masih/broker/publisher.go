package broker

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

type Publisher struct {
	PeerOperations
	Id                  int
	NrMessagesToPublish uint
	MessageSize         uint64
	Results             *Result
	SyncMutex           *sync.Mutex
	SyncCond            *sync.Cond
	NrReadyPeers        *int
	NrDonePeers         *int
}

func (publisher *Publisher) StartPublishing(nrPeers int) {
	publisher.SetupPublishRoutine()

	var (
		send   = publisher.SendChannel()
		errors = publisher.ErrorChannel()
	)

	// Update ready value for notifying in ready-state
	publisher.SyncMutex.Lock()
	*publisher.NrReadyPeers += 1
	publisher.SyncCond.Signal()
	publisher.SyncMutex.Unlock()

	// Waiting for all peers to be ready
	publisher.SyncMutex.Lock()
	for nrPeers != *publisher.NrReadyPeers {
		publisher.SyncCond.Wait()
	}
	publisher.SyncCond.Broadcast()
	publisher.SyncMutex.Unlock()
	fmt.Printf("Publisher id: %d starting publishing...\n", publisher.Id)

	var nrPublishedMessages uint = 0

	message := make([]byte, publisher.MessageSize)

	startTime := time.Now().UnixNano()
	defer publisher.saveResultsAndNotify(startTime)
	defer publisher.DoneChannel()

	// Publish messages until PublisherCounter == NumMessages
	for nrPublishedMessages < publisher.NrMessagesToPublish {
		// Puts the actual time in the message
		binary.PutVarint(message, time.Now().UnixNano())
		select {
		case send <- message:
			nrPublishedMessages += 1
			continue
		case err := <-errors:
			// TODO: If a publish fails, a subscriber will probably deadlock.
			// The best option is probably to signal back to the client that
			// a publisher failed so it can orchestrate a shutdown.
			fmt.Fprintf(os.Stderr, "Producer %d failed to send message: %s\n", publisher.Id, err.Error())
			return
		}
	}
}

func (publisher *Publisher) saveResultsAndNotify(startTime int64) {
	// Blocking until all messages are delivered
	<-publisher.DeliveredChannel()
	endTime := time.Now().UnixNano()

	ms := float32(endTime-startTime) / 1000000
	publisher.Results = &Result{
		PeerID:     publisher.Id,
		Duration:   ms,
		Throughput: 1000 * float32(publisher.NrMessagesToPublish) / ms,
	}

	fmt.Printf("\nPublisher id: %d has completed\n", publisher.Id)

	publisher.SyncMutex.Lock()
	*publisher.NrDonePeers += 1
	publisher.SyncCond.Signal()
	publisher.SyncMutex.Unlock()
}
