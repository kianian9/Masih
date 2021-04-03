package broker

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	// Maximum bytes we will get behind before we start slowing down publishing.
	maxBytesBehind = 1024 * 1024 * 2 // 2MB

	// Time to delay publishing when we are behind.
	delay = 1 * time.Millisecond
)

type Publisher struct {
	PeerOperations
	Id                       int
	NrMessagesToPublish      uint
	MessageSize              uint64
	Results                  *Result
	SyncMutex                *sync.Mutex
	SyncCond                 *sync.Cond
	NrReadyPeers             *int
	NrDonePeers              *int
	SubNrConsumedMessagesArr []*uint
	SubscriberDoneArr        []*bool
	NrPublishers             int
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

			// Congestion control, minimizing producer throughput if
			// consumers getting too far behind so they can catch up
			var leastReceives *uint = nil
			// Finding subscriber with least receives
			publisher.SyncMutex.Lock()
			for i := 0; i < len(publisher.SubNrConsumedMessagesArr); i++ {
				subscriberMessageReceived := publisher.SubNrConsumedMessagesArr[i]
				subscriberDone := publisher.SubscriberDoneArr[i]
				if leastReceives == nil && !*subscriberDone {
					leastReceives = subscriberMessageReceived
				} else if !*subscriberDone && leastReceives != nil && *leastReceives > *subscriberMessageReceived {
					leastReceives = subscriberMessageReceived
				}
			}
			var val uint = 0
			if leastReceives != nil {
				val = *leastReceives
			}
			publisher.SyncMutex.Unlock()

			if val != 0 {
				// Approximately total amount of bytes published
				approxBytesSent := uint64(nrPublishedMessages) * uint64(publisher.NrPublishers) * publisher.MessageSize
				leastBytesReceived := uint64(val) * publisher.MessageSize

				// If approx total amount of byte sent - threshold val is greater than
				//  subscribers' received number of bytes, then slow down and let receiver bounce back
				if approxBytesSent-maxBytesBehind > leastBytesReceived {
					time.Sleep(delay)
				}
			}
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
