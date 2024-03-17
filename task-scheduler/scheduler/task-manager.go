package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	qm "tskscheduler/storage"
	cnfg "tskscheduler/task-scheduler/config"
	model "tskscheduler/task-scheduler/model"
)

type TaskManager struct {
	tasksWeight map[string]model.TaskWeight
	servers     map[string]model.Servers
	consumer    *qm.Consumer
	producer    *qm.Producer
	supClient   *qm.SupabaseClient
	receive     chan []byte
	done        chan int
}

func InitManager(consumer *qm.Consumer, producer *qm.Producer, supClient *qm.SupabaseClient, done chan int, config *cnfg.Config) *TaskManager {
	servers := make(map[string]model.Servers)
	tasksWeight := make(map[string]model.TaskWeight)
	for _, server := range config.Servers {
		servers[server.Id] = server
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
	tm.receiveTask()
}

func (tm *TaskManager) receiveTask() {
	select {
	case <-tm.done:
		return
	case taskData := <-tm.receive:
		var task model.Task
		json.Unmarshal(taskData, &task)
		if task.Meta.Action == "ADD_TASK" {
			go tm.assignTask(&task)
		}
		fmt.Printf("end of the task assignment case")
	}
}

func (tm *TaskManager) assignTask(task *model.Task) {
	id, err := tm.supClient.SaveTask(&task.Meta)
	if err != nil {
		log.Printf("error saving task %v\n", err)
		return
	}
	taskWeight, ok := tm.tasksWeight[task.Meta.TaskType]
	var minServer model.Servers
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

func (tm *TaskManager) completeTask(serverId, taskId string) {

}
