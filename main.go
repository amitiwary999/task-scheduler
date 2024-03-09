package main

import (
	"fmt"
	"os"
	amqp "tskscheduler/task-scheduler/storage"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
	}
	done := make(chan int)
	rabbitmqUri := os.Getenv("RABBITMQ_URL")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	exchangeType := os.Getenv("RABBITMQ_EXCHANGE_TYPE")
	queue := os.Getenv("RABBITMQ_QUEUE")
	exchangeKey := os.Getenv("RABBITMQ_EXCHANGE_KEY")
	consumerTag := os.Getenv("RABBITMQ_CONSUMER_TAG")
	consumer, err := amqp.NewConsumer(rabbitmqUri, exchange, exchangeType, queue, exchangeKey, consumerTag, done)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	} else {
		consumer.SetupCloseHandler()
	}

	<-done

}
