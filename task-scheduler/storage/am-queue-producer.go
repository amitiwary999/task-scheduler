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

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan int
}

func (producer *Producer) SetupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Ctrl+C pressed in Terminal")
		producer.done <- 1
		os.Exit(0)
	}()
}

func NewProducer(done chan int) (*Producer, error) {
	amqpURI := os.Getenv("RABBITMQ_URL")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	exchangeType := os.Getenv("RABBITMQ_EXCHANGE_TYPE")
	queueName := os.Getenv("RABBITMQ_QUEUE_JOB_SERVER")

	c := &Producer{
		conn:    nil,
		channel: nil,
		done:    done,
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("sample-Producer")
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
	_, err = c.channel.QueueDeclare(
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

	return c, nil
}

func (c *Producer) SendTaskMessage(taskId, serverId string) {
	fmt.Printf("send the task %v %v \n", serverId, taskId)
	body := fmt.Sprintf(`{"server":%v, "task":%v}`, serverId, taskId)
	exchange := os.Getenv("RABBITMQ_EXCHANGE")

	err := c.channel.PublishWithContext(context.Background(), exchange, serverId, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})

	if err != nil {
		fmt.Printf("error sendig task to server %v\n", err)
	}
}
