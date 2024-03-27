package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan int)
	producerQueueName := os.Getenv("RABBITMQ_QUEUE")
	producer, err := storage.NewProducer(done, producerQueueName)
	consumer, err := storage.NewConsumer(done)

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

		if err != nil {
			fmt.Printf("amq connection error %v\n", err)
		}
		cordinator := task.NewCordinator(consumer, producer, supa, done, consumerKey)
		cordinator.Start()
	}
	<-gracefulShutdown
	if len(serversData) > 0 {
		serverId := serversData[0].ServerId
		serverLeaveData := model.JoinData{
			ServerId: serverId,
			Status:   0,
		}
		updateErr := supa.UpdateServerStatus(serverId, 0)
		if updateErr != nil {
			fmt.Printf("failed to remove the server status on leave")
		}
		producer.SendServerJoinMessage(&serverLeaveData)
	}
	producer.ShutDown()
	consumer.Shutdown()
	done <- 1

}
