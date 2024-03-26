package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	cnfg "tskscheduler/task-scheduler/config"
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
	localCache, localCacheInitError := storage.NewLocalCache()
	if localCacheInitError != nil {
		fmt.Printf("local cache init error %v\n", localCacheInitError)
	}
	taskM := manag.InitManager(consumer, producer, supa, localCache, done, cnfg.LoadConfig())
	taskM.StartManager()

	<-gracefulShutdown
	done <- 1
}
