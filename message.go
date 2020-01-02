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
	BatchTotal  int
	BatchItem   int
	sync.Mutex
}

// Actions
func (message *Message) Ack(multiple bool) {

	message.Lock()
	defer message.Unlock()

	if message.ActionTaken {
		return
	}

	err := message.Message.Ack(multiple)
	if err != nil {
		logError(err)
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
		logError(err)
	} else {
		message.ActionTaken = true
	}
}

// Helpers
func (message *Message) SendToQueue(channels ...*Channel) {

	// Send to back of current queue if none specified
	if len(channels) == 0 {
		channels = []*Channel{message.Channel}
	}

	//
	var err error

	var ack = true
	for _, channel := range channels {
		err = channel.produceMessage(message)
		if err != nil {
			logError(err)
			ack = false
		}
	}

	if ack {
		message.Ack(false)
	}
}

func (message *Message) PercentOfBatch() float64 {
	return float64(message.BatchItem) / float64(message.BatchTotal) * 100
}

// Headers
const (
	headerAttempt    = "attempt"
	headerFirstSeen  = "first-seen"
	headerLastSeen   = "last-seen"
	headerFirstQueue = "first-queue"
	headerLastQueue  = "last-queue"
)

func (message Message) Attempt() (i int) {

	i = 1
	if val, ok := message.Message.Headers[headerAttempt]; ok {
		if val2, ok2 := val.(int32); ok2 {
			i = int(val2)
		}
	}

	message.Message.Headers[headerAttempt] = i

	return i
}

func (message Message) FirstSeen() (i time.Time) {

	i = time.Now()
	if val, ok := message.Message.Headers[headerFirstSeen]; ok {
		if val2, ok2 := val.(int64); ok2 {
			i = time.Unix(val2, 0)
		}
	}

	message.Message.Headers[headerFirstSeen] = i

	return i
}

func (message Message) LastSeen() (i time.Time) {

	i = time.Now()
	if val, ok := message.Message.Headers[headerLastSeen]; ok {
		if val2, ok2 := val.(int64); ok2 {
			i = time.Unix(val2, 0)
		}
	}

	message.Message.Headers[headerLastSeen] = i

	return i
}

func (message Message) FirstQueue() (i QueueName) {

	i = ""
	if val, ok := message.Message.Headers[headerFirstQueue]; ok {
		if val2, ok2 := val.(string); ok2 {
			i = QueueName(val2)
		}
	}
	return i
}

func (message Message) LastQueue() (i QueueName) {

	i = ""
	if val, ok := message.Message.Headers[headerLastQueue]; ok {
		if val2, ok2 := val.(string); ok2 {
			i = QueueName(val2)
		}
	}
	return i
}
