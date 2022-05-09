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
	InChannel       = "IN_CHANNEL"
	KafkaUrlEnvName = "KAFKA_URL"
)

var (
	queueName = os.Getenv(QueueEnvName)
	natsUrl   = os.Getenv(NatsUrlEnvName)
	kafkaUrl  = os.Getenv(KafkaUrlEnvName)
	logLevel  = "Info"
	inChannel = os.Getenv(InChannel)
)

type mainfluxMessageHandler struct {
	writer *kafka.Writer
}

func (m *mainfluxMessageHandler) f(msg messaging.Message) error {
	return m.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(msg.Publisher),
		Value: msg.Payload,
	})
}

func main() {
	mainfluxLog, err := logger.New(os.Stdout, logLevel)
	if err != nil {
		log.Fatal("Got error creating mainflux logger", err.Error())
	}
	pubSub, err := nats.NewPubSub(natsUrl, queueName, mainfluxLog)
	defer pubSub.Close()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaUrl),
		Topic:                  queueName,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	h := mainfluxMessageHandler{
		writer: w,
	}

	if pubSub.Subscribe("channels."+inChannel, h.f) != nil {
		mainfluxLog.Error("Fail subscribe to mainflux channel:" + err.Error())
		return
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()
}
