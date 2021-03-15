package broker

import (
	"encoding/binary"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"log"
	"sync"
	"time"
)

type Subscriber struct {
	PeerOperations
	Id              int
	NumMessages     uint
	MessageSize     int64
	HasStarted      bool
	Started         int64
	Stopped         int64
	ConsumerCounter *uint
	Results         *Result
	//SubscriberResultMutex	*sync.Mutex Kanske inte behövs då varje subscriber har sin egna results
	SyncMutex    *sync.Mutex
	SyncCond     *sync.Cond
	NrReadyPeers *int
	NrDonePeers  *int
}

const (
	maxRecordableLatencyMS = 300000
	sigFigs                = 5
)

func (subscriber *Subscriber) StartSubscribing(nrPeers int) {
	// Update ready value for notifying in ready-state
	subscriber.SyncMutex.Lock()
	*subscriber.NrReadyPeers += 1
	subscriber.SyncCond.Signal()
	subscriber.SyncMutex.Unlock()

	// Waiting for all peers to be ready
	subscriber.SyncMutex.Lock()
	for nrPeers != *subscriber.NrReadyPeers {
		//fmt.Printf("Subscriber id: %d waiting for other peers...\n", subscriber.Id)
		subscriber.SyncCond.Wait()
	}
	subscriber.SyncCond.Broadcast()
	subscriber.SyncMutex.Unlock()

	fmt.Printf("Subscriber id: %d starting consuming...\n", subscriber.Id)

	var nrConsumedMessages uint = 0
	latencies := hdrhistogram.New(0, maxRecordableLatencyMS, sigFigs)
	subscriber.Started = time.Now().UnixNano()
	for nrConsumedMessages < subscriber.NumMessages {
		message, err := subscriber.ReceiveMessage()
		now := time.Now().UnixNano()
		if err != nil {
			log.Printf("Subscriber error: %s", err.Error())
			subscriber.Results = &Result{Err: err.Error()}
			return
		}
		then, _ := binary.Varint(message)
		latencies.RecordValue((now - then) / 1000000)
		nrConsumedMessages += 1
	}
	subscriber.Stopped = time.Now().UnixNano()
	durationMS := float32(subscriber.Stopped-subscriber.Started) / 1000000
	subscriber.Results = &Result{
		PeerID:     subscriber.Id,
		Duration:   durationMS,
		Throughput: 1000 * float32(subscriber.NumMessages) / durationMS,
		Latency: &latencyResults{
			Min:    latencies.Min(),
			Q1:     latencies.ValueAtQuantile(25),
			Q2:     latencies.ValueAtQuantile(50),
			Q3:     latencies.ValueAtQuantile(75),
			Max:    latencies.Max(),
			Mean:   latencies.Mean(),
			StdDev: latencies.StdDev(),
		},
	}

	fmt.Printf("\nSubscriber id: %d has completed\n", subscriber.Id)

	subscriber.SyncMutex.Lock()
	*subscriber.NrDonePeers += 1
	subscriber.SyncCond.Signal()
	subscriber.SyncMutex.Unlock()

}
