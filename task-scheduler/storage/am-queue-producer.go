package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	producerModel "tskscheduler/servers/model"
	model "tskscheduler/task-scheduler/model"

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

func NewProducer(done chan int, queueName string) (*Producer, error) {
	amqpURI := os.Getenv("RABBITMQ_URL")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	exchangeType := os.Getenv("RABBITMQ_EXCHANGE_TYPE")

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

func (c *Producer) SendTaskMessage(taskId, routingKey string) {
	fmt.Printf("send the task %v %v \n", routingKey, taskId)
	bodyModel := &model.TaskMessage{
		TaskId:   taskId,
		ServerId: routingKey,
	}
	body, err := json.Marshal(bodyModel)
	if err != nil {
		fmt.Printf("task maessage body parse error %v\n", err)
	}
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	publishErr := c.channel.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})

	if publishErr != nil {
		fmt.Printf("error sendig task to server %v\n", err)
	}
}

func (c *Producer) SendTaskCompleteMessage(taskData *producerModel.Task) {
	producerKey := os.Getenv("RABBITMQ_EXCHANGE_KEY")
	fmt.Printf("send complete task, action %v taskid %v\n", taskData.Meta.Action, taskData.Id)
	body, err := json.Marshal(&taskData)
	if err != nil {
		fmt.Printf("complete task maessage body parse error %v\n", err)
	}
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	publishErr := c.channel.PublishWithContext(context.Background(), exchange, producerKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})

	if publishErr != nil {
		fmt.Printf("error sendig complete task to server %v\n", err)
	}
}
