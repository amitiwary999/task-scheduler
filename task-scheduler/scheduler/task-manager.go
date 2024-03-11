package scheduler

import (
	"log"
	cnfg "tskscheduler/task-scheduler/config"
	model "tskscheduler/task-scheduler/model"
	qm "tskscheduler/task-scheduler/storage"
)

type TaskManager struct {
	taskWeight map[string]model.TaskWeight
	servers    map[string]model.Servers
	consumer   *qm.Consumer
	supClient  *qm.SupabaseClient
	receive    chan string
	done       chan int
}

func InitManager(consumer *qm.Consumer, supClient *qm.SupabaseClient, done chan int, config *cnfg.Config) *TaskManager {
	servers := make(map[string]model.Servers)
	tasksWeight := make(map[string]model.TaskWeight)

	for _, server := range config.Servers {
		servers[server.Id] = server
	}

	for _, taskWeight := range config.TaskWeight {
		tasksWeight[taskWeight.Id] = taskWeight
	}

	return &TaskManager{
		taskWeight: tasksWeight,
		servers:    servers,
		consumer:   consumer,
		supClient:  supClient,
		receive:    make(chan string),
		done:       done,
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
			log.Printf("task data %v\n", taskData)
		}
	}
}
