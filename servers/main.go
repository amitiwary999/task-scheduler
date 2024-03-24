package main

import (
	"encoding/json"
	"fmt"
	"os"
	model "tskscheduler/servers/model"
	task "tskscheduler/servers/tasks"
	storage "tskscheduler/task-scheduler/storage"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("/Users/amitt/Documents/Personal Data/personalproj/TaskScheduler/.env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
		return
	}

	done := make(chan int)
	producerQueueName := os.Getenv("RABBITMQ_QUEUE")
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
	unusedServerByte, err := supa.GetUnusedServer()
	if err != nil {
		fmt.Printf("error in getting single unused server %v\n", err)
	}
	var serversData []model.JoinData
	json.Unmarshal(unusedServerByte, &serversData)
	fmt.Printf("servers data %v\n", serversData)
	if len(serversData) > 0 {
		serverData := serversData[0]
		consumerKey := serverData.ServerId
		updateErr := supa.UpdateServerStatus(serverData.ServerId, 1)
		if updateErr != nil {
			fmt.Printf("error in updating the server join status %v\n", updateErr)
		}
		serverData.Status = 1
		producer.SendServerJoinMessage(&serverData)

		consumer, err := storage.NewConsumer(done)
		if err != nil {
			fmt.Printf("amq connection error %v\n", err)
		} else {
			consumer.SetupCloseHandler()
		}
		cordinator := task.NewCordinator(consumer, producer, supa, done, consumerKey)
		cordinator.Start()
	}
	<-done
}
