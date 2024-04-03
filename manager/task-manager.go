package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	model "github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
)

type TaskManager struct {
	tasksWeight         map[string]model.TaskWeight
	servers             map[string]*model.Servers
	consumer            util.AMQPConsumer
	producer            util.AMQPProducer
	supClient           util.SupabaseClient
	ReceiveTask         chan []byte
	ReceiveCompleteTask chan []byte
	serverJoin          chan []byte
	done                chan int
	lock                sync.Mutex
}

func InitManager(consumer util.AMQPConsumer, producer util.AMQPProducer, supClient util.SupabaseClient, done chan int) *TaskManager {
	servers := make(map[string]*model.Servers)
	tasksWeight := make(map[string]model.TaskWeight)

	var taskWeightConfig []model.TaskWeight
	taskConfigByte, _ := supClient.GetTaskConfig()
	json.Unmarshal(taskConfigByte, &taskWeightConfig)
	for _, taskWeight := range taskWeightConfig {
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
		ReceiveTask:         make(chan []byte),
		ReceiveCompleteTask: make(chan []byte),
		serverJoin:          make(chan []byte),
		done:                done,
	}
}

func (tm *TaskManager) StartManager() {
	key := util.RABBITMQ_EXCHANGE_KEY
	queueName := util.RABBITMQ_TASK_QUEUE
	completeTaskKey := util.RABBITMQ_COMPLETE_TASK_EXCHANGE_KEY
	taskCompleteQueue := util.RABBITMQ_TASK_COMPLETE_QUEUE
	go tm.consumer.Handle(tm.ReceiveTask, queueName, key, util.TaskConsumerTag)
	go tm.receiveNewTask()
	go tm.consumer.Handle(tm.ReceiveCompleteTask, taskCompleteQueue, completeTaskKey, util.CompleteTaskConsumerTag)
	go tm.receiveCompleteTaskFunc()
	go tm.consumer.ServerJoinHandle(tm.serverJoin, util.NewServerJoinTag)
	go tm.receiveServerJoinMessage()
}

func (tm *TaskManager) receiveNewTask() {
	for {
		select {
		case <-tm.done:
			return
		case taskData := <-tm.ReceiveTask:
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
		case taskData := <-tm.ReceiveCompleteTask:
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
				tm.lock.Lock()
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
				tm.lock.Unlock()
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
