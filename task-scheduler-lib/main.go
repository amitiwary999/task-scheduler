package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	manag "tskscheduler/task-scheduler/scheduler"
	storage "tskscheduler/task-scheduler/storage"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
	}

	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan int)
	producerQueueName := os.Getenv("RABBITMQ_QUEUE_JOB_SERVER")
	consumer, err := storage.NewConsumer(done)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	}
	producer, err := storage.NewProducer(done, producerQueueName)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	}
	supa, error := storage.NewSupabaseClient()
	if error != nil {
		fmt.Printf("supabase cloient failed %v\n", error)
	}
	taskM := manag.InitManager(consumer, producer, supa, done)
	taskM.StartManager()

	<-gracefulShutdown
	close(done)
	consumer.Shutdown()
	producer.Shutdown()
}
