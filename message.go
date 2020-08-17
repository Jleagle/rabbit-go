package rabbit

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Message struct {
	Channel     *Channel
	Message     *amqp.Delivery
	ActionTaken bool
	sync.Mutex
}

// Actions
func (message *Message) Ack(multiple ...bool) {

	message.Lock()
	defer message.Unlock()

	if message.ActionTaken {
		return
	}

	err := message.Message.Ack(len(multiple) > 0 && multiple[0])
	if err != nil {
		message.Channel.connection.logError(err)
	} else {
		message.ActionTaken = true
	}
}

func (message *Message) Nack(multiple bool, requeue bool) {

	message.Lock()
	defer message.Unlock()

	if message.ActionTaken {
		return
	}

	err := message.Message.Nack(multiple, requeue)
	if err != nil {
		message.Channel.connection.logError(err)
	} else {
		message.ActionTaken = true
	}
}

// Helpers
func (message *Message) SendToQueueAndAck(channel *Channel, mutator ProduceOptions) (err error) {

	err = channel.produceMessage(message, mutator)
	if err == nil {
		message.Ack()
	}
	return err
}

// Headers
const (
	headerAttempt    = "attempt"
	headerFirstSeen  = "first-seen"
	headerLastSeen   = "last-seen"
	headerFirstQueue = "first-queue"
	headerLastQueue  = "last-queue"
	headerUUID       = "uuid"
)

func (message *Message) Attempt() (i int) {

	i = 1
	if val, ok := message.Message.Headers[headerAttempt]; ok {
		if val2, ok2 := val.(int32); ok2 {
			i = int(val2)
		}
	}

	message.Message.Headers[headerAttempt] = i

	return i
}

func (message *Message) FirstSeen() (t time.Time) {

	var i int64
	if val, ok := message.Message.Headers[headerFirstSeen]; ok {
		if val2, ok2 := val.(int64); ok2 {
			i = val2
		}
		if val2, ok2 := val.(time.Time); ok2 {
			i = val2.Unix()
		}
	}

	message.Message.Headers[headerFirstSeen] = i

	return time.Unix(i, 0)
}

func (message *Message) LastSeen() (t time.Time) {

	var i int64
	if val, ok := message.Message.Headers[headerLastSeen]; ok {
		if val2, ok2 := val.(int64); ok2 {
			i = val2
		}
		if val2, ok2 := val.(time.Time); ok2 {
			i = val2.Unix()
		}
	}

	message.Message.Headers[headerLastSeen] = i

	return time.Unix(i, 0)
}

func (message *Message) FirstQueue() (i QueueName) {

	i = ""
	if val, ok := message.Message.Headers[headerFirstQueue]; ok {
		if val2, ok2 := val.(string); ok2 {
			i = QueueName(val2)
		}
	}
	return i
}

func (message *Message) LastQueue() (i QueueName) {

	i = ""
	if val, ok := message.Message.Headers[headerLastQueue]; ok {
		if val2, ok2 := val.(string); ok2 {
			i = QueueName(val2)
		}
	}
	return i
}

func (message *Message) UUID() string {

	if val, ok := message.Message.Headers[headerUUID]; ok {
		if val2, ok2 := val.(string); ok2 {
			return val2
		}
	}
	return ""
}
