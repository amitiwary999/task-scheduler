package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	delivery <-chan amqp.Delivery
	tag      string
	done     chan int
}

func (consumer *Consumer) SetupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Ctrl+C pressed in Terminal")
		if err := consumer.Shutdown(); err != nil {
			log.Fatalf("error during shutdown: %s", err)
		}
		os.Exit(0)
	}()
}

func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string, done chan int) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    done,
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("sample-consumer")
	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.DialConfig(amqpURI, config)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		log.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
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
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
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
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	c.delivery = deliveries
	//go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	c.done <- 1
	return nil
}

func (c *Consumer) Handle(data chan []byte) {
	select {
	case <-c.done:
		return
	case <-c.delivery:
		for d := range c.delivery {
			data <- d.Body
			log.Printf("consume body %v\n", d.Body)
			d.Ack(true)
		}
	}
}

func (c *Consumer) SendTaskMessage(taskId, serverId string) {
	body := fmt.Sprintf(`{"server":%v, "task":%v}`, serverId, taskId)
	var queueName string
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	if serverId == "server1" {
		queueName = os.Getenv("RABBITMQ_QUEUE_JOB_SERVER_1")
	} else if serverId == "server2" {
		queueName = os.Getenv("RABBITMQ_QUEUE_JOB_SERVER_2")
	} else if serverId == "server3" {
		queueName = os.Getenv("RABBITMQ_QUEUE_JOB_SERVER_3")
	} else {
		return
	}
	c.channel.PublishWithContext(context.Background(), exchange, queueName, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}
