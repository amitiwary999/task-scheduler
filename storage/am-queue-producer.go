package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	util "github.com/amitiwary999/task-scheduler/util"

	model "github.com/amitiwary999/task-scheduler/model"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan int
}

var connectionProducer = "task-scheduler-producer"

func NewProducer(done chan int, queueName string, rabbitmqUrl string) (*Producer, error) {
	amqpURI := rabbitmqUrl
	exchange := util.RABBITMQ_EXCHANGE
	exchangeType := util.RABBITMQ_EXCHANGE_TYPE

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

func (c *Producer) Shutdown() {
	if err := c.conn.Close(); err != nil {
		fmt.Printf("AMQP connection close error: %s", err)
	}
	fmt.Printf("AMQP producer shutdown\n")
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
	exchange := util.RABBITMQ_EXCHANGE
	publishErr := c.channel.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})

	if publishErr != nil {
		fmt.Printf("error sendig task to server %v\n", err)
	}
}
