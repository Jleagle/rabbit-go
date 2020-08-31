package rabbit

import (
	"fmt"
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

	var err error

	// Producer
	connectionConsumerConfig := ConnectionConfig{
		Address:  localDSN,
		ConnType: Producer,
	}

	producerConnection, err := NewConnection(connectionConsumerConfig)
	if err != nil {
		t.Error(err)
		return
	}

	channelProducerConfig := ChannelConfig{
		Connection:    producerConnection,
		QueueName:     queueName,
		ConsumerName:  consumerName,
		PrefetchCount: 1,
		UpdateHeaders: true,
	}

	producerChannel, err = NewChannel(channelProducerConfig)
	if err != nil {
		t.Error(queueName, err)
	}

	// Consumer
	connectionProducerConfig := ConnectionConfig{
		Address:  localDSN,
		ConnType: Consumer,
	}

	consumerConnection, err := NewConnection(connectionProducerConfig)
	if err != nil {
		t.Error(err)
		return
	}

	channelConsumerConfig := ChannelConfig{
		Connection:    consumerConnection,
		QueueName:     queueName,
		ConsumerName:  consumerName,
		PrefetchCount: 1,
		Handler:       handler,
	}

	consumerChannel, err = NewChannel(channelConsumerConfig)
	if err != nil {
		t.Error(queueName, err)
	}

	go consumerChannel.Consume()

	// Auto produce messages
	go func() {
		var i int
		for {
			i++
			err := producerChannel.Produce(i, nil)
			if err != nil {
				t.Error(err)
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

	fmt.Println("Body: " + string(message.Message.Body))
	message.Ack()
}
