# Masih
Masih - Message Queue Benchmarking Tool is a concurrent evaluation tool that could be used for benchmarking **Kafka**, **NATS Streaming (STAN)**, and **RabbitMQ** message queues.
Masih provides automated orchestration for benchmarking message queues in parallelized configurations, where it is possible to evaluate a message broker with arbitrarily many producers and consumers with a single command.

## Installation
Masih uses a Go module that collects all dependencies needed in order to run the application. The benchmarking framework can be installed by running following from masih directory:
```bash
$ go mod download
```


## Execution
The program can be builded by entering following from masih directory:
```bash
$ go build -o masih
```
Furthermore, users of this benchmarking tool can set the following flags:
* **-broker**: The broker system that is going to be evaluated, supporting Kafka, RabbitMQ and STAN, where the default value is set to *RabbitMQ*
* **-host**: The host address for communicating with the broker system, default set to *localhost*
* **-port**: The port that is used by the broker system for handling incoming client-requests, default set to *5672* (RabbitMQ:s port by default)
* **-username**: An authorized RabbitMQ username that is used for creating queues,  by default set to *guest* (which only works with local brokers)
* **-password**: A RabbitMQ password (which corresponds to the username) that is used for creating queues, by default set to *guest*
* **-messageSize**: The size of each published message in bytes, by default set to publish *1000* bytes messages
* **-numMessages**: Total number of messages to publish by the publishers, by default set to publish *500 000* messages
* **-producers**: Total number of producers to use in the evaluation, by default set to *1* producer
* **-consumers**: Total number of consumers to use in the evaluation, by default set to *1* consumer
* **-queueType**: The queue type to use for the RabbitMQ system, supporting classic mirrored queues (CLASSIC) and quorum based queues *QUORUM* (which is used as default)
* **-clusterID**: The cluster id for the STAN brokers, set to *stan* as default
* **-topic**: The topic or subject to publish and consume from in Kafka and STAN, set to *test* as default

An example of how to run the built program is shown below.
```shell
$ ./masih \
    -broker=kafka \
    -host=192.168.20.101 \
    -port=9094 \
    -messageSize=1000 \
    -numMessages=7000000 \
    -producers=5 \
    -consumers=5 \
    -topic=kianian-test
```

## Contributors
This benchmarking tool is inspired by [Flotilla benchmarking tool](https://github.com/tylertreat/Flotilla), which in contrast to Masih is a distributed benchmarking tool.

