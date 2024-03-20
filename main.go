package main

import (
	"fmt"
	"os"
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

	done := make(chan int)
	key := os.Getenv("RABBITMQ_EXCHANGE_KEY")
	queueName := os.Getenv("RABBITMQ_QUEUE")
	producerQueueName := os.Getenv("RABBITMQ_QUEUE_JOB_SERVER")
	consumer, err := storage.NewConsumer(done, queueName, key)
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
	redisClient, redisInitErr := storage.NewRedisClient()
	if redisInitErr != nil {
		fmt.Printf("redis init error %v\n", redisInitErr)
	}
	taskM := manag.InitManager(consumer, producer, supa, redisClient, done, cnfg.LoadConfig())
	taskM.StartManager()
	<-done

}
