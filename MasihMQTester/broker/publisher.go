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
	//PublisherCounter	*uint
	Results *Result
	//PublisherResultMutex	*sync.Mutex Kanske inte behövs då varje publisher har sin egna results
	//Mu          	*sync.Mutex
	SyncMutex    *sync.Mutex
	SyncCond     *sync.Cond
	NrReadyPeers *int
	NrDonePeers  *int
}

func (publisher *Publisher) StartPublishing(nrPeers int) {
	publisher.SetupPublishRoutine()
	defer publisher.DoneChannel()

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
		//fmt.Printf("Publisher id: %d waiting for other peers...\n", publisher.Id)
		publisher.SyncCond.Wait()
	}
	publisher.SyncCond.Broadcast()
	publisher.SyncMutex.Unlock()
	fmt.Printf("Publisher id: %d starting publishing...\n", publisher.Id)

	var nrPublishedMessages uint = 0

	message := make([]byte, publisher.MessageSize)
	// Publish messages until PublisherCounter == NumMessages
	startTime := time.Now().UnixNano()
	t1 := time.Now().UnixNano() / 1000000000
	messSent := 0
	for nrPublishedMessages < publisher.NrMessagesToPublish {
		// Random generating a message for publish
		//message := make([]byte, publisher.MessageSize)
		//rand.Seed(time.Now().UnixNano())
		//rand.Read(message)

		// Puts the actual time in the message
		binary.PutVarint(message, time.Now().UnixNano())
		select {

		case send <- message:
			t2 := time.Now().UnixNano() / 1000000000
			if t2-t1 >= 1 {
				fmt.Println("Throughput(mess/sec): ", (int(nrPublishedMessages) - messSent))
				messSent = int(nrPublishedMessages)
				t1 = t2
			}
			nrPublishedMessages += 1
			continue
		case err := <-errors:
			// TODO: If a publish fails, a subscriber will probably deadlock.
			// The best option is probably to signal back to the client that
			// a publisher failed so it can orchestrate a shutdown.
			fmt.Fprintf(os.Stderr, "Failed to send message: %s", err.Error())
			return
		}

		// Asynchronous publish
		//publisher.PeerOperations.PublishMessage(message)
		//err := publisher.PeerOperations.PublishMessage(message)
		/*
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: Publisher %d failed publishing - %s\n", publisher.Id ,err)
				continue
			}
		*/

		/*
			publisher.PublisherResultMutex.Lock()
			if *publisher.PublisherCounter < publisher.NrMessagesToPublish {
				// Take time here...
				err := publisher.PeerOperations.PublishMessage(message)


				if err == nil {
					*publisher.PublisherCounter += 1
				}
			}
			publisher.PublisherResultMutex.Unlock()
		*/
	}
	endTime := time.Now().UnixNano()

	ms := float32(endTime-startTime) / 1000000
	publisher.Results = &Result{
		PeerID:     publisher.Id,
		Duration:   ms,
		Throughput: 1000 * float32(publisher.NrMessagesToPublish) / ms,
	}

	/*
		publisher.PublisherResultMutex.Lock()
		publisher.Results = &Result{
			Duration:   ms,
			Throughput: 1000 * float32(publisher.NrMessagesToPublish) / ms,
		}
		publisher.PublisherResultMutex.Unlock()
	*/
	fmt.Printf("\nPublisher id: %d has completed\n", publisher.Id)

	publisher.SyncMutex.Lock()
	*publisher.NrDonePeers += 1
	publisher.SyncCond.Signal()
	publisher.SyncMutex.Unlock()

	/*

		message := make([]byte, 1000)
		rand.Seed(time.Now().UnixNano())
		rand.Read(message)
		fmt.Println(message)
	*/

}
