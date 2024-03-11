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
	rabbitmqUri := os.Getenv("RABBITMQ_URL")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	exchangeType := os.Getenv("RABBITMQ_EXCHANGE_TYPE")
	queue := os.Getenv("RABBITMQ_QUEUE")
	exchangeKey := os.Getenv("RABBITMQ_EXCHANGE_KEY")
	consumerTag := os.Getenv("RABBITMQ_CONSUMER_TAG")
	supabaseKey := os.Getenv("SUPABASE_KEY")
	supabaseUrl := os.Getenv("SUPABASE_URL")

	done := make(chan int)
	consumer, err := storage.NewConsumer(rabbitmqUri, exchange, exchangeType, queue, exchangeKey, consumerTag, done)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	} else {
		consumer.SetupCloseHandler()
	}

	supa, error := storage.NewSupabaseClient(supabaseUrl, supabaseKey)
	if error != nil {
		fmt.Printf("supabase cloient failed %v\n", error)
	}
	manag.InitManager(consumer, supa, done, cnfg.LoadConfig())
	<-done

}
