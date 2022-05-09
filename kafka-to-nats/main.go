package main

import (
	"context"
	"github.com/mainflux/mainflux/logger"
	"github.com/mainflux/mainflux/pkg/messaging"
	"github.com/mainflux/mainflux/pkg/messaging/nats"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	QueueEnvName    = "QUEUE_NAME"
	NatsUrlEnvName  = "NATS_URL"
	OutChannel      = "OUT_CHANNEL"
	KafkaUrlEnvName = "KAFKA_URL"
	GroupId         = "GROUP_ID"
)

var (
	queueName  = os.Getenv(QueueEnvName)
	natsUrl    = os.Getenv(NatsUrlEnvName)
	kafkaUrl   = os.Getenv(KafkaUrlEnvName)
	logLevel   = "Info"
	outChannel = os.Getenv(OutChannel)
	groupId    = os.Getenv(GroupId)
)

func main() {
	mainfluxLog, e := logger.New(os.Stdout, logLevel)
	if e != nil {
		log.Fatal("Got error creating mainflux logger", e.Error())
	}
	pubSub, e := nats.NewPubSub(natsUrl, queueName, mainfluxLog)
	if e != nil {
		log.Fatal("Got error creating mainflux pubSub", e.Error())
	}

	defer pubSub.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaUrl},
		GroupID:  groupId,
		Topic:    queueName,
		MaxBytes: 10e6, // 10MB
	})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	receive := true

	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		receive = false
		cancel()
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()

	for receive {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			mainfluxLog.Error("Cannot fetch message: " + err.Error())
			break
		}

		mainfluxMessage := messaging.Message{
			Channel: outChannel,
			Payload: m.Value,
		}
		if err := pubSub.Publish(outChannel, mainfluxMessage); err != nil {
			mainfluxLog.Error("Error publishing message to mainflux: " + err.Error())
			break
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			mainfluxLog.Error("failed to commit messages: " + err.Error())
			break
		}
	}

	if err := r.Close(); err != nil {
		mainfluxLog.Error("failed to close reader: " + err.Error())
	}
}
