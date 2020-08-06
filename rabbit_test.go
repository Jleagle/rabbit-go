package rabbit

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
)

const (
	localDSN               = "amqp://guest:guest@localhost:5672"
	queueName    QueueName = "rabbit-go-test"
	consumerName           = "rabbit-go-consumer"
)

var (
	producerChannel *Channel
	consumerChannel *Channel
)

func TestConnection(t *testing.T) {

	SetDebug(true)

	var err error

	// Producer
	producerConnection, err := NewConnection(localDSN, Producer, amqp.Config{})
	if err != nil {
		logInfo(err)
		return
	}

	producerChannel, err = NewChannel(producerConnection, queueName, consumerName, 1, nil, true)
	if err != nil {
		logError(string(queueName), err)
	}

	// Consumer
	consumerConnection, err := NewConnection(localDSN, Consumer, amqp.Config{})
	if err != nil {
		logInfo(err)
		return
	}

	consumerChannel, err = NewChannel(consumerConnection, queueName, consumerName, 1, handler, false)
	if err != nil {
		logError(string(queueName), err)
	}

	go consumerChannel.Consume()

	// Auto produce messages
	go func() {
		var i int
		for {
			i++
			err := producerChannel.Produce(i, nil)
			if err != nil {
				logError(err)
			}
			<-time.NewTimer(time.Second * 2).C
		}
	}()

	go func() {
		<-time.NewTimer(time.Second * 5).C
		consumerConnection.closeChan <- &amqp.Error{Code: 404, Reason: "testing", Server: true, Recover: true}
	}()

	select {}
}

func handler(message *Message) {

	logInfo(string(message.Message.Body))
	message.Ack()
}
