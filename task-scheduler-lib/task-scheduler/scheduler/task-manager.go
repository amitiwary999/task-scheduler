package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	cnfg "tskscheduler/task-scheduler/config"
	model "tskscheduler/task-scheduler/model"
	qm "tskscheduler/task-scheduler/storage"
)

type TaskManager struct {
	tasksWeight         map[string]model.TaskWeight
	servers             map[string]*model.Servers
	consumer            *qm.Consumer
	producer            *qm.Producer
	supClient           *qm.SupabaseClient
	receiveTask         chan []byte
	receiveCompleteTask chan []byte
	serverJoin          chan []byte
	done                chan int
}

func InitManager(consumer *qm.Consumer, producer *qm.Producer, supClient *qm.SupabaseClient, done chan int, config *cnfg.Config) *TaskManager {
	servers := make(map[string]*model.Servers)
	tasksWeight := make(map[string]model.TaskWeight)

	for _, taskWeight := range config.TaskWeight {
		tasksWeight[taskWeight.Type] = taskWeight
	}

	var serversJoinData []model.JoinData
	serversByte, serversErr := supClient.GetAllUsedServer()

	if serversErr != nil {
		fmt.Printf("error in get all used servers %v\n", serversErr)
	} else {
		json.Unmarshal(serversByte, &serversJoinData)

		for _, serverJonData := range serversJoinData {
			server := model.Servers{
				Id:   serverJonData.ServerId,
				Load: 0,
			}
			servers[serverJonData.ServerId] = &server
		}
	}

	return &TaskManager{
		tasksWeight:         tasksWeight,
		servers:             servers,
		producer:            producer,
		consumer:            consumer,
		supClient:           supClient,
		receiveTask:         make(chan []byte),
		receiveCompleteTask: make(chan []byte),
		serverJoin:          make(chan []byte),
		done:                done,
	}
}

func (tm *TaskManager) StartManager() {
	key := os.Getenv("RABBITMQ_EXCHANGE_KEY")
	queueName := os.Getenv("RABBITMQ_QUEUE")
	completeTaskKey := os.Getenv("RABBITMQ_COMPLETE_TASK_EXCHANGE_KEY")
	taskCompleteQueue := os.Getenv("RABBITMQ_TASK_COMPLETE_QUEUE")
	go tm.consumer.Handle(tm.receiveTask, queueName, key, tm.consumer.TaskConsumerTag)
	go tm.receiveNewTask()
	go tm.consumer.Handle(tm.receiveCompleteTask, taskCompleteQueue, completeTaskKey, tm.consumer.CompleteTaskConsumerTag)
	go tm.receiveCompleteTaskFunc()
	go tm.consumer.ServerJoinHandle(tm.serverJoin, tm.consumer.ServerJoinConsumerTag)
	go tm.receiveServerJoinMessage()
}

func (tm *TaskManager) receiveNewTask() {
	for {
		select {
		case <-tm.done:
			return
		case taskData := <-tm.receiveTask:
			var task model.Task
			err := json.Unmarshal(taskData, &task)
			if err != nil {
				fmt.Printf("json unmarshal error in receive task %v\n", err)
			} else {
				if task.Meta.Action == "ADD_TASK" {
					go tm.assignTask(&task)
				}
			}
		}
	}
}

func (tm *TaskManager) receiveCompleteTaskFunc() {
	for {
		select {
		case <-tm.done:
			return
		case taskData := <-tm.receiveCompleteTask:
			var task model.CompleteTask
			err := json.Unmarshal(taskData, &task)
			if err != nil {
				fmt.Printf("json unmarshal error in receive task %v\n", err)
			} else {
				if task.Meta.Action == "COMPLETE_TASK" {
					go tm.completeTask(task)
				}
			}
		}
	}
}

func (tm *TaskManager) receiveServerJoinMessage() {
	for {
		select {
		case <-tm.done:
			return
		case joinData := <-tm.serverJoin:
			var join model.JoinData
			err := json.Unmarshal(joinData, &join)
			if err != nil {
				fmt.Printf("json unmarshal error in server join %v\n", err)
			} else {
				fmt.Printf("receive server join message %v server %v\n", join.Status, join.ServerId)
				if join.Status == 1 {
					server := model.Servers{
						Id:   join.ServerId,
						Load: 0,
					}
					tm.servers[join.ServerId] = &server
				} else {
					serverId := join.ServerId
					for _, server := range tm.servers {
						if server.Id == serverId {
							delete(tm.servers, serverId)
							break
						}
					}
				}
			}
		}
	}
}

func (tm *TaskManager) assignTask(task *model.Task) {
	id, err := tm.supClient.SaveTask(&task.Meta)
	if err != nil {
		log.Printf("error saving task %v\n", err)
		return
	}
	taskWeight, ok := tm.tasksWeight[task.Meta.TaskType]
	var minServer *model.Servers
	minLoadVal := 10000000
	if ok {
		for _, server := range tm.servers {
			fmt.Printf("load of the server in assign task %v server %v\n", server.Load, server.Id)
			if taskWeight.Weight+server.Load < minLoadVal {
				minServer = server
				minLoadVal = server.Load + taskWeight.Weight
			}
		}
		if minServer != nil {
			minServer.Load = minLoadVal
			tm.producer.SendTaskMessage(id, minServer.Id)
		}
	}
}

func (tm *TaskManager) completeTask(task model.CompleteTask) {
	serverId := task.Meta.ServerId
	taskType := task.Meta.TaskType
	server := tm.servers[serverId]
	taskWeight, ok := tm.tasksWeight[taskType]
	if ok {
		fmt.Printf("server load before reduce %v for id %v\n", server.Load, server.Id)
		server.Load = server.Load - taskWeight.Weight
		fmt.Printf("server load reduced %v for id %v\n", server.Load, server.Id)
	}
	tm.supClient.UpdateTaskComplete(task.Id)
	fmt.Printf("complete task action\n")
}
