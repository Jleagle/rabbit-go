package rabbit

import (
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/streadway/amqp"
)

type ConnType string

const (
	Consumer ConnType = "consumer"
	Producer ConnType = "producer"
)

type ConnectionConfig struct {
	Address  string
	ConnType ConnType
	Config   amqp.Config
	LogInfo  func(i ...interface{})
	LogError func(i ...interface{})
}

func NewConnection(config ConnectionConfig) (c *Connection, err error) {

	if config.LogInfo == nil {
		config.LogInfo = func(i ...interface{}) {
			fmt.Println(i...)
		}
	}

	if config.LogError == nil {
		config.LogError = func(i ...interface{}) {
			fmt.Println(i...)
		}
	}

	connection := &Connection{
		dial:     config.Address,
		config:   config.Config,
		connType: config.ConnType,
		logInfo:  config.LogInfo,
		logError: config.LogError,
	}

	connection.connect()

	go func() {
		for {
			amqpErr := <-connection.closeChan
			connection.connection = nil
			connection.logError("Rabbit connection disconnected", amqpErr)
			connection.connect()
		}
	}()

	return connection, nil
}

type Connection struct {
	dial       string
	connection *amqp.Connection
	config     amqp.Config
	closeChan  chan *amqp.Error
	connType   ConnType
	logInfo    func(...interface{})
	logError   func(...interface{})
	sync.Mutex
}

func (connection *Connection) isReady() bool {
	return connection.connection != nil && !connection.connection.IsClosed()
}

func (connection *Connection) connect() {

	connection.Lock()
	defer connection.Unlock()

	if connection.isReady() {
		return
	}

	connection.logInfo("Creating Rabbit connection (" + connection.connType + ")")

	operation := func() (err error) {

		// Connect
		connection.connection, err = amqp.DialConfig(connection.dial, connection.config)
		if err != nil {
			return err
		}

		// Set new close channel
		connection.closeChan = make(chan *amqp.Error)
		_ = connection.connection.NotifyClose(connection.closeChan)

		return err
	}

	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = time.Second * 1
	policy.MaxInterval = time.Minute * 5
	policy.MaxElapsedTime = 0

	err := backoff.RetryNotify(operation, policy, func(err error, t time.Duration) { connection.logInfo("Trying to connect to Rabbit", err) })
	if err != nil {
		connection.logError(err)
	} else {
		connection.logInfo("Rabbit conn connected (" + connection.connType + ")")
	}
}
