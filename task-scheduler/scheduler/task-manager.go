package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	cnfg "tskscheduler/task-scheduler/config"
	model "tskscheduler/task-scheduler/model"
	qm "tskscheduler/task-scheduler/storage"

	"github.com/rabbitmq/amqp091-go"
)

type TaskManager struct {
	tasksWeight map[string]model.TaskWeight
	servers     []model.Servers
	consumer    *qm.Consumer
	supClient   *qm.SupabaseClient
	receive     chan string
	done        chan int
}

func InitManager(consumer *qm.Consumer, supClient *qm.SupabaseClient, done chan int, config *cnfg.Config) *TaskManager {
	servers := make([]model.Servers, 0, len(config.Servers))
	tasksWeight := make(map[string]model.TaskWeight)

	for i, server := range config.Servers {
		servers[i] = server
	}

	for _, taskWeight := range config.TaskWeight {
		tasksWeight[taskWeight.Type] = taskWeight
	}

	return &TaskManager{
		tasksWeight: tasksWeight,
		servers:     servers,
		consumer:    consumer,
		supClient:   supClient,
		receive:     make(chan string),
		done:        done,
	}
}

func (tm *TaskManager) StartManager() {
	tm.receiveTask(tm.receive, tm.done)
	go tm.consumer.Handle(tm.receive)
}

func (tm *TaskManager) receiveTask(recChan chan string, done chan int) {
	select {
	case <-done:
		return
	case <-recChan:
		for taskData := range recChan {
			var taskMeta model.TaskMeta
			json.Unmarshal([]byte(taskData), &taskMeta)
			tm.assignTask(&taskMeta)
			log.Printf("task data %v\n", taskData)
		}
	}
}

func (tm *TaskManager) assignTask(taskMeta *model.TaskMeta) {
	id, err := tm.supClient.SaveTask(taskMeta)
	if err != nil {
		log.Printf("error saving task %v\n", err)
	}
	taskWeight, ok := tm.tasksWeight[taskMeta.TaskType]
	minId := tm.servers[0].Id
	minLoadVal := tm.servers[0].Load
	if ok {
		for _, server := range tm.servers {
			if taskWeight.Weight+server.Load < minLoadVal {
				minId = server.Id
				minLoadVal = server.Load + taskWeight.Weight
			}
		}
	}
	tm.sendTaskMessage(minId, id)
	log.Printf("taskid %v serverid %v\n", id, minId)
}

func (tm *TaskManager) sendTaskMessage(taskId, serverId string) {
	body := fmt.Sprintf(`{"server":%v, "task":%v}`, serverId, taskId)
	var queueName string
	exchange := os.Getenv("RABBITMQ_EXCHANGE")
	if serverId == "server1" {
		queueName = os.Getenv("RABBITMQ_QUEUE_JOB_SERVER_1")
	} else if serverId == "server2" {
		queueName = os.Getenv("RABBITMQ_QUEUE_JOB_SERVER_2")
	} else if serverId == "server3" {
		queueName = os.Getenv("RABBITMQ_QUEUE_JOB_SERVER_3")
	} else {
		return
	}
	tm.consumer.Channel.PublishWithContext(context.Background(), exchange, queueName, false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})
}
