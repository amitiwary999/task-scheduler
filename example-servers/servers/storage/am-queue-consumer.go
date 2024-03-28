package storage

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan int
}

var taskConsumerTag = "task-consumer"
var newServerJoinTag = "server-join"
var connectionName = "task-scheduler-consumer"

func NewConsumer(done chan int) (*Consumer, error) {
	amqpURI := os.Getenv("RABBITMQ_URL")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	exchangeType := os.Getenv("RABBITMQ_EXCHANGE_TYPE")

	c := &Consumer{
		conn:    nil,
		channel: nil,
		done:    done,
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName(connectionName)
	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.DialConfig(amqpURI, config)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	go func() {
		log.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("exchange Declare: %s", err)
	}

	return c, nil
}

func (c *Consumer) Shutdown() error {
	if err := c.channel.Cancel(taskConsumerTag, true); err != nil {
		return fmt.Errorf("task consumer cancel failed: %s", err)
	}
	if err := c.channel.Cancel(newServerJoinTag, true); err != nil {
		return fmt.Errorf("server join consumer cancel failed: %s", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	fmt.Printf("AMQP consumer shutdown\n")

	return nil
}

func (c *Consumer) Handle(data chan []byte, queueName string, key string) error {
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return fmt.Errorf("queue Bind: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", taskConsumerTag)
	deliveries, err := c.channel.Consume(
		queue.Name,      // name
		taskConsumerTag, // consumerTag,
		false,           // autoAck
		false,           // exclusive
		false,           // noLocal
		false,           // noWait
		nil,             // arguments
	)
	if err != nil {
		return fmt.Errorf("queue consume: %s", err)
	}
	for {
		select {
		case <-c.done:
			return nil
		case d := <-deliveries:
			if len(d.Body) > 0 {
				data <- d.Body
			}
			d.Ack(true)
		}
	}
}
