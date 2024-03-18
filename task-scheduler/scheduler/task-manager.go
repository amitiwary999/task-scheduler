package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	cnfg "tskscheduler/task-scheduler/config"
	model "tskscheduler/task-scheduler/model"
	qm "tskscheduler/task-scheduler/storage"
)

type TaskManager struct {
	tasksWeight map[string]model.TaskWeight
	servers     map[string]*model.Servers
	consumer    *qm.Consumer
	producer    *qm.Producer
	supClient   *qm.SupabaseClient
	receive     chan []byte
	done        chan int
}

func InitManager(consumer *qm.Consumer, producer *qm.Producer, supClient *qm.SupabaseClient, done chan int, config *cnfg.Config) *TaskManager {
	servers := make(map[string]*model.Servers)
	tasksWeight := make(map[string]model.TaskWeight)
	for _, server := range config.Servers {
		servers[server.Id] = &server
	}

	for _, taskWeight := range config.TaskWeight {
		tasksWeight[taskWeight.Type] = taskWeight
	}
	return &TaskManager{
		tasksWeight: tasksWeight,
		servers:     servers,
		producer:    producer,
		consumer:    consumer,
		supClient:   supClient,
		receive:     make(chan []byte),
		done:        done,
	}
}

func (tm *TaskManager) StartManager() {
	go tm.consumer.Handle(tm.receive)
	go tm.receiveTask()
}

func (tm *TaskManager) receiveTask() {
	for {
		select {
		case <-tm.done:
			return
		case taskData := <-tm.receive:
			var task model.Task
			err := json.Unmarshal(taskData, &task)
			if err != nil {
				fmt.Printf("json unmarshal error in receive task %v\n", err)
			} else {
				if task.Meta.Action == "ADD_TASK" {
					go tm.assignTask(&task)
				} else if task.Meta.Action == "COMPLETE_TASK" {
					go tm.completeTask(task)
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
			if taskWeight.Weight+server.Load < minLoadVal {
				minServer = server
				minLoadVal = server.Load + taskWeight.Weight
			}
		}
	}
	minServer.Load = minLoadVal
	tm.producer.SendTaskMessage(id, minServer.Id)
}

func (tm *TaskManager) completeTask(task model.Task) {
	serverId := task.Meta.TaskId
	taskType := task.Meta.TaskType
	server := tm.servers[serverId]
	taskWeight, ok := tm.tasksWeight[taskType]
	if ok {
		server.Load = server.Load - taskWeight.Weight
		fmt.Printf("server load reduced %v for id %v\n", server.Load, server.Id)
	}
	fmt.Printf("complete task action\n")
}
