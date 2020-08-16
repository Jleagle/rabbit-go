package rabbit

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type (
	QueueName string
	Handler   func(message *Message)
)

func NewChannel(connection *Connection, queueName QueueName, consumerName string, prefetchCount int, handler Handler, updateHeaders bool) (c *Channel, err error) {

	channel := &Channel{
		connection:    connection,
		QueueName:     queueName,
		ConsumerName:  consumerName,
		prefetchCount: prefetchCount,
		handler:       handler,
		updateHeaders: updateHeaders,
	}

	channel.connect()

	// For producer channels
	if connection.connType == Producer {
		go func() {
			for {
				amqpErr := <-channel.closeChan
				channel.onDisconnect(amqpErr)
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
	updateHeaders bool
	connectLock   sync.Mutex
}

func (channel *Channel) isReady() bool {

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

type ProduceOptions = func(amqp.Publishing) amqp.Publishing

func (channel *Channel) produceMessage(message *Message, mutator ProduceOptions) error {

	// Headers
	message.Message.Headers = channel.setHeaders(message.Message.Headers, channel.updateHeaders)

	//
	msg := amqp.Publishing{
		Headers:      message.Message.Headers,
		DeliveryMode: message.Message.DeliveryMode,
		ContentType:  message.Message.ContentType,
		Body:         message.Message.Body,
	}

	if mutator != nil {
		msg = mutator(msg)
	}

	return channel.channel.Publish("", string(channel.QueueName), false, false, msg)
}

func (channel *Channel) Produce(body interface{}, mutator ProduceOptions) error {

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		Headers:      channel.setHeaders(nil, channel.updateHeaders),
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         b,
	}

	if mutator != nil {
		msg = mutator(msg)
	}

	return channel.channel.Publish("", string(channel.QueueName), false, false, msg)
}

func (channel *Channel) onDisconnect(amqpErr *amqp.Error) {

	channel.isOpen = false
	channel.channel = nil

	logError("Rabbit channel disconnected ("+channel.QueueName+")", amqpErr)

	channel.connect()
}

func (channel *Channel) setHeaders(headers amqp.Table, update bool) amqp.Table {

	if headers == nil {
		headers = amqp.Table{}
	}

	var ok bool

	_, ok = headers[headerAttempt]
	if !ok {
		headers[headerAttempt] = 0
	}

	_, ok = headers[headerFirstSeen]
	if !ok {
		headers[headerFirstSeen] = 0
	}

	_, ok = headers[headerLastSeen]
	if !ok {
		headers[headerLastSeen] = 0
	}

	_, ok = headers[headerUUID]
	if !ok {
		id, _ := uuid.NewRandom()
		headers[headerUUID] = id.String()
	}

	//
	if update {

		//
		if val, ok := headers[headerAttempt].(int); ok {
			headers[headerAttempt] = val + 1
		} else {
			headers[headerAttempt] = 1
		}

		//
		if val, ok := headers[headerFirstSeen].(int); !ok || val == 0 {
			headers[headerFirstSeen] = time.Now().Unix()
		}

		//
		headers[headerLastSeen] = time.Now().Unix()

		//
		if val, ok := headers[headerFirstQueue]; !ok || val == "" {
			headers[headerFirstQueue] = string(channel.QueueName)
		}

		//
		headers[headerLastQueue] = string(channel.QueueName)
	}

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

		func() {
			for {
				select {
				case amqpErr := <-channel.closeChan:

					channel.onDisconnect(amqpErr)
					return

				case msg, open := <-msgs:

					if open && channel.connection.isReady() && channel.handler != nil {

						channel.handler(&Message{Channel: channel, Message: &msg})
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
