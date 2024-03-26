package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	producerModel "tskscheduler/servers/model"
	model "tskscheduler/task-scheduler/model"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan int
}

var connectionProducer = "task-scheduler-producer"

func (producer *Producer) SetupCloseHandler() {
	go func() {
		<-producer.done
		producer.ShutDown()
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
	config.Properties.SetClientConnectionName(connectionProducer)
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

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	return c, nil
}

func (c *Producer) ShutDown() {
	if err := c.conn.Close(); err != nil {
		fmt.Printf("AMQP connection close error: %s", err)
	}
	fmt.Printf("AMQP producer shutdown")
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
		fmt.Printf("error sendig complete task to server %v\n", publishErr)
	}
}

func (c *Producer) SendServerJoinMessage(serverJoinData *producerModel.JoinData) {
	fmt.Printf("send server join/leave message status %v \n", serverJoinData.Status)
	key := os.Getenv("RABBITMQ_SERVER_JOIN_EXCHANGE_KEY")
	body, err := json.Marshal(&serverJoinData)
	if err != nil {
		fmt.Printf("faield to marshal server join/leave status %v data producer %v\n", serverJoinData.Status, err)
	}
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	publishErr := c.channel.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})

	if publishErr != nil {
		fmt.Printf("error sendig server join/leave status %v message %v\n", serverJoinData.Status, publishErr)
	}
}
