package rabbit

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
)

const (
	localDSN               = "localhost:5672"
	queueName    QueueName = "queue-name"
	consumerName           = "consumer-name"
)

var (
	producerChannel *Channel
	consumerChannel *Channel
)

func TestConnection(t *testing.T) {

	var err error

	// Producers
	producerConnection, err := NewConnection(localDSN, Producer, amqp.Config{})
	if err != nil {
		logInfo(err)
		return
	}

	producerChannel, err = NewChannel(producerConnection, queueName, consumerName, 10, 1, nil, true)
	if err != nil {
		logError(string(queueName), err)
	}

	// Consumers
	consumerConnection, err := NewConnection(localDSN, Consumer, amqp.Config{})
	if err != nil {
		logInfo(err)
		return
	}

	consumerChannel, err = NewChannel(consumerConnection, queueName, consumerName, 10, 1, handler, false)
	if err != nil {
		logError(string(queueName), err)
	}

	go consumerChannel.Consume()

	// Auto produce messages
	go func() {
		var i int
		for {
			i++
			err := producerChannel.Produce(i)
			if err != nil {
				logError(err)
			}
			time.Sleep(time.Second * 5)
		}
	}()

	select {}
}

func handler(messages []*Message) {

	for _, message := range messages {
		logInfo(string(message.Message.Body))
		message.Ack()
	}
}