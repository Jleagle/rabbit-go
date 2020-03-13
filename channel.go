package rabbit

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/streadway/amqp"
)

type (
	QueueName string
	Handler   func(message []*Message)
)

func NewChannel(connection *Connection, queueName QueueName, consumerName string, prefetchCount int, batchSize int, handler Handler, updateHeaders bool) (c *Channel, err error) {

	channel := &Channel{
		connection:    connection,
		QueueName:     queueName,
		ConsumerName:  consumerName,
		prefetchCount: prefetchCount,
		batchSize:     batchSize,
		handler:       handler,
		updateHeaders: updateHeaders,
	}

	channel.connect()

	// For producer channels
	if connection.connType == Producer {
		go func() {
			for {
				select {
				case amqpErr, _ := <-channel.closeChan:
					channel.onDisconnect(amqpErr)
				}
			}
		}()
	}

	return channel, nil
}

type Channel struct {
	QueueName     QueueName
	ConsumerName  string
	connection    *Connection
	channel       *amqp.Channel
	closeChan     chan *amqp.Error
	handler       Handler
	isOpen        bool
	prefetchCount int
	batchSize     int
	updateHeaders bool
	connectLock   sync.Mutex
}

func (channel Channel) isReady() bool {

	return channel.connection.isReady() && channel.isOpen && channel.channel != nil
}

func (channel *Channel) connect() {

	channel.connectLock.Lock()
	defer channel.connectLock.Unlock()

	if channel.isReady() {
		return
	}

	operation := func() (err error) {

		if !channel.connection.isReady() {
			return errors.New("waiting for connection")
		}

		// Connect
		c, err := channel.connection.connection.Channel()
		if err != nil {
			return err
		}

		// Set prefetch
		if channel.prefetchCount > 0 {
			err = c.Qos(channel.prefetchCount, 0, false)
			if err != nil {
				return err
			}
		}

		// Set new close channel
		channel.closeChan = make(chan *amqp.Error)
		_ = c.NotifyClose(channel.closeChan)

		channel.channel = c

		// Queue
		_, err = channel.channel.QueueDeclare(string(channel.QueueName), true, false, false, false, nil)
		if err != nil {
			return err
		}

		//
		channel.isOpen = true

		return nil
	}

	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = time.Second * 1
	policy.MaxInterval = time.Minute * 5
	policy.MaxElapsedTime = 0

	err := backoff.RetryNotify(operation, policy, func(err error, t time.Duration) { logWarning("Connecting to channel: ", err) })
	if err != nil {
		logError(err)
	} else {
		logInfo("Rabbit chan connected (" + string(channel.connection.connType) + "/" + string(channel.QueueName) + ")")
	}
}

func (channel *Channel) produceMessage(message *Message) error {

	// Headers
	if channel.updateHeaders {
		message.Message.Headers = channel.prepareHeaders(message.Message.Headers)
	}

	//
	return channel.channel.Publish("", string(channel.QueueName), false, false, amqp.Publishing{
		Headers:      message.Message.Headers,
		DeliveryMode: message.Message.DeliveryMode,
		ContentType:  message.Message.ContentType,
		Body:         message.Message.Body,
	})
}

func (channel *Channel) Produce(message interface{}) error {

	b, err := json.Marshal(message)
	if err != nil {
		return err
	}

	headers := amqp.Table{}
	if channel.updateHeaders {
		headers = channel.prepareHeaders(headers)
	}

	return channel.channel.Publish("", string(channel.QueueName), false, false, amqp.Publishing{
		Headers:      headers,
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         b,
	})
}

func (channel *Channel) onDisconnect(amqpErr *amqp.Error) {

	channel.isOpen = false
	channel.channel = nil

	logError("Rabbit channel disconnected ("+channel.QueueName+")", amqpErr)

	channel.connect()
}

func (channel Channel) prepareHeaders(headers amqp.Table) amqp.Table {

	if headers == nil {
		headers = amqp.Table{}
	}

	//
	attemptSet := false
	attempt, ok := headers[headerAttempt]
	if ok {
		if val, ok2 := attempt.(int); ok2 {
			headers[headerAttempt] = val + 1
			attemptSet = true
		}
	}
	if !attemptSet {
		headers[headerAttempt] = 1
	}

	//
	_, ok = headers[headerFirstSeen]
	if !ok {
		headers[headerFirstSeen] = time.Now().Unix()
	}

	//
	headers[headerLastSeen] = time.Now().Unix()

	//
	_, ok = headers[headerFirstQueue]
	if !ok {
		headers[headerFirstQueue] = string(channel.QueueName)
	}

	//
	headers[headerLastQueue] = string(channel.QueueName)

	return headers
}

func (channel *Channel) Consume() {

	for {

		if !channel.isReady() {
			logInfo("Can't consume when channel is nil/closed")

			<-time.NewTimer(time.Second * 5).C

			continue
		}

		msgs, err := channel.channel.Consume(string(channel.QueueName), channel.ConsumerName, false, false, false, false, nil)
		if err != nil {
			logError("Getting Rabbit channel chan", err)
			continue
		}

		// In a anon function so can return at anytime
		func() {

			var messages []*Message

			for {
				select {
				case amqpErr, _ := <-channel.closeChan:

					channel.onDisconnect(amqpErr)
					return

				case msg, open := <-msgs:
					if open && channel.connection.isReady() {
						messages = append(messages, &Message{
							Channel: channel,
							Message: &msg,
						})
					}
				}

				if len(messages) > 0 && len(messages) >= channel.batchSize {

					if channel.handler != nil {

						// Fill in batch info
						for k := range messages {
							messages[k].BatchTotal = len(messages)
							messages[k].BatchItem = k + 1
						}

						//
						channel.handler(messages)
						messages = nil
					}
				}
			}
		}()

		<-time.NewTimer(time.Second * 5).C
	}
}

func (channel *Channel) Inspect() (amqp.Queue, error) {

	return channel.channel.QueueInspect(string(channel.QueueName))
}
