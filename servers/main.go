package main

import (
	"fmt"
	"log"
	"os"
	task "tskscheduler/servers/tasks"
	storage "tskscheduler/storage"

	"github.com/joho/godotenv"
)

func main() {
	argv := os.Args[1:]
	if len(argv) == 0 {
		log.Printf("please provide the queue name")
		return
	}
	err := godotenv.Load("/Users/amitt/Documents/Personal Data/personalproj/TaskScheduler/.env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
		return
	}

	done := make(chan int)
	// producerKey := os.Getenv("RABBITMQ_EXCHANGE_KEY")
	consumerKey := argv[0]
	queueName := os.Getenv("RABBITMQ_QUEUE_JOB_SERVER")
	producerQueueName := os.Getenv("RABBITMQ_QUEUE")
	consumer, err := storage.NewConsumer(done, queueName, consumerKey)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	} else {
		consumer.SetupCloseHandler()
	}
	producer, err := storage.NewProducer(done, producerQueueName)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	} else {
		producer.SetupCloseHandler()
	}
	supa, error := storage.NewSupabaseClient()
	if error != nil {
		fmt.Printf("supabase cloient failed %v\n", error)
	}
	cordinator := task.NewCordinator(consumer, producer, supa, done)
	cordinator.Start()
}
