package storage

import (
	"fmt"
	"log"

	util "github.com/amitiwary999/TaskScheduler/task-scheduler-lib/task-scheduler/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan int
}

var connectionName = "task-scheduler-consumer"

func NewConsumer(done chan int, rabbitmqUrl string) (*Consumer, error) {
	amqpURI := rabbitmqUrl
	exchange := util.RABBITMQ_EXCHANGE
	exchangeType := util.RABBITMQ_EXCHANGE_TYPE

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

func (c *Consumer) Shutdown() {
	if err := c.channel.Cancel(util.TaskConsumerTag, true); err != nil {
		fmt.Printf("task consumer cancel failed: %s", err)
	}
	if err := c.channel.Cancel(util.NewServerJoinTag, true); err != nil {
		fmt.Printf("server join consumer cancel failed: %s", err)
	}
	if err := c.conn.Close(); err != nil {
		fmt.Printf("AMQP connection close error: %s", err)
	}
	fmt.Printf("AMQP consumer shutdown\n")
}

func (c *Consumer) Handle(data chan []byte, queueName string, key string, consumerTag string) error {
	exchange := util.RABBITMQ_EXCHANGE
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

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", consumerTag)
	deliveries, err := c.channel.Consume(
		queue.Name,  // name
		consumerTag, // consumerTag,
		false,       // autoAck
		false,       // exclusive
		false,       // noLocal
		false,       // noWait
		nil,         // arguments
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

func (c *Consumer) ServerJoinHandle(serverJoin chan []byte, consumerTag string) error {
	serverJoinQueue := util.SERVER_JOIN_RABBITMQ_QUEUE
	serverJoinKey := util.RABBITMQ_SERVER_JOIN_EXCHANGE_KEY
	exchange := util.RABBITMQ_EXCHANGE

	queue, err := c.channel.QueueDeclare(serverJoinQueue, true, false, false, false, nil)
	if err != nil {
		fmt.Printf("queue declare failed %v\n", err)
		return err
	}

	bindErr := c.channel.QueueBind(queue.Name, serverJoinKey, exchange, false, nil)
	if bindErr != nil {
		fmt.Printf("failed to bind queue %v error %v\n", queue.Name, bindErr)
		return bindErr
	}
	deliveries, err := c.channel.Consume(
		queue.Name,  // name
		consumerTag, // consumerTag,
		false,       // autoAck
		false,       // exclusive
		false,       // noLocal
		false,       // noWait
		nil,         // arguments
	)
	if err != nil {
		return err
	}
	for {
		select {
		case <-c.done:
			return nil
		case d := <-deliveries:
			if len(d.Body) > 0 {
				serverJoin <- d.Body
			}
			d.Ack(true)
		}
	}
}
