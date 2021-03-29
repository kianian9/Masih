package broker

import (
	"crypto/rand"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
	"strconv"
)

type PeerOperations interface {
	ReceiveMessage() ([]byte, error)
	SetupPublishRoutine()
	SendChannel() chan<- []byte
	ErrorChannel() <-chan error
	DoneChannel()
}

type Results struct {
	PublisherResults  []*Result
	SubscriberResults []*Result
}

type MQSettings struct {
	BrokerName  string
	BrokerHost  string
	BrokerPort  string
	Username    string
	Password    string
	MessageSize uint64
	NumMessages uint
	Producers   uint
	Consumers   uint
	QueueType   string
	ClusterID   string
	Topic       string
}

type Result struct {
	PeerID     int
	Duration   float32         `json:"duration,omitempty"`
	Throughput float32         `json:"throughput,omitempty"`
	Latency    *latencyResults `json:"latency,omitempty"`
	Err        string          `json:"error,omitempty"`
}

type latencyResults struct {
	Min    int64   `json:"min"`
	Q1     int64   `json:"q1"`
	Q2     int64   `json:"q2"`
	Q3     int64   `json:"q3"`
	Max    int64   `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"std_dev"`
}

func GenerateName() string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 32)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func DividePeerMessages(nrPeers int, numMessages uint) []uint {
	messagesPerPeer := make([]uint, nrPeers)

	mess := numMessages / uint(nrPeers)
	remainder := numMessages % uint(nrPeers)

	// Compensating remainder loss
	for i := 0; i < nrPeers; i++ {
		val := mess
		if remainder > 0 {
			val += 1
			remainder--
		}
		messagesPerPeer[i] = val
	}
	return messagesPerPeer
}

func (results *Results) PrintResults() {
	var (
		producerData   = [][]string{}
		pubDurations   = float32(0)
		pubThroughputs = float32(0)
		i              = 1
	)
	if len(results.PublisherResults) > 0 {
		for _, result := range results.PublisherResults {
			pubDurations += result.Duration
			pubThroughputs += result.Throughput
			producerData = append(producerData, []string{
				strconv.Itoa(i),
				strconv.FormatBool(result.Err != ""),
				strconv.FormatFloat(float64(result.Duration), 'f', 3, 32),
				strconv.FormatFloat(float64(result.Throughput), 'f', 3, 32),
			})
			i++
		}

		avgPubDuration := pubDurations / (float32(i) - 1)
		avgPubThroughput := pubThroughputs / (float32(i) - 1)
		producerData = append(producerData, []string{
			"AVG",
			"",
			strconv.FormatFloat(float64(avgPubDuration), 'f', 3, 32),
			strconv.FormatFloat(float64(avgPubThroughput), 'f', 3, 32),
		})

		printTable([]string{
			"Producer",
			"Error",
			"Duration",
			"Throughput (msg/sec)",
		}, producerData)
	}

	consumerData := [][]string{}
	i = 1
	var (
		subDurations   = float32(0)
		subThroughputs = float32(0)
		subMins        = int64(0)
		subQ1s         = int64(0)
		subQ2s         = int64(0)
		subQ3s         = int64(0)
		subMaxes       = int64(0)
		subMeans       = float64(0)
		subIQRs        = int64(0)
		subStdDevs     = float64(0)
	)
	if len(results.SubscriberResults) > 0 {
		for _, result := range results.SubscriberResults {
			subDurations += result.Duration
			subThroughputs += result.Throughput
			subMins += result.Latency.Min
			subQ1s += result.Latency.Q1
			subQ2s += result.Latency.Q2
			subQ3s += result.Latency.Q3
			subMaxes += result.Latency.Max
			subMeans += result.Latency.Mean
			subIQRs += result.Latency.Q3 - result.Latency.Q1
			subStdDevs += result.Latency.StdDev
			consumerData = append(consumerData, []string{
				strconv.Itoa(i),
				strconv.FormatBool(result.Err != ""),
				strconv.FormatFloat(float64(result.Duration), 'f', 3, 32),
				strconv.FormatFloat(float64(result.Throughput), 'f', 3, 32),
				strconv.FormatInt(result.Latency.Min, 10),
				strconv.FormatInt(result.Latency.Q1, 10),
				strconv.FormatInt(result.Latency.Q2, 10),
				strconv.FormatInt(result.Latency.Q3, 10),
				strconv.FormatInt(result.Latency.Max, 10),
				strconv.FormatFloat(result.Latency.Mean, 'f', 3, 64),
				strconv.FormatInt(result.Latency.Q3-result.Latency.Q1, 10),
				strconv.FormatFloat(result.Latency.StdDev, 'f', 3, 64),
			})
			i++
		}

		var (
			avgSubDuration   = subDurations / (float32(i) - 1)
			avgSubThroughput = subThroughputs / (float32(i) - 1)
			avgSubMin        = subMins / (int64(i) - 1)
			avgSubQ1         = subQ1s / (int64(i) - 1)
			avgSubQ2         = subQ2s / (int64(i) - 1)
			avgSubQ3         = subQ3s / (int64(i) - 1)
			avgSubMaxes      = subMaxes / (int64(i) - 1)
			avgSubMeans      = subMeans / (float64(i) - 1)
			avgSubIQRs       = subIQRs / (int64(i) - 1)
			avgSubStdDevs    = subStdDevs / (float64(i) - 1)
		)
		consumerData = append(consumerData, []string{
			"AVG",
			"",
			strconv.FormatFloat(float64(avgSubDuration), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubThroughput), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubMin), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubQ1), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubQ2), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubQ3), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubMaxes), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubMeans), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubIQRs), 'f', 3, 32),
			strconv.FormatFloat(float64(avgSubStdDevs), 'f', 3, 32),
		})
		printTable([]string{
			"Consumer",
			"Error",
			"Duration",
			"Throughput (msg/sec)",
			"Min",
			"Q1",
			"Q2",
			"Q3",
			"Max",
			"Mean",
			"IQR",
			"Std Dev",
		}, consumerData)
	}
	fmt.Println("All units ms unless noted otherwise")
	fmt.Printf("********************************************************** DONE **********************************************************\n\n\n")

}

func printTable(headers []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	for _, row := range data {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.Render()
}
