package scheduler

import (
	"fmt"

	manager "github.com/amitiwary999/task-scheduler/manager"
	model "github.com/amitiwary999/task-scheduler/model"
	storage "github.com/amitiwary999/task-scheduler/storage"
)

type TaskScheduler struct {
	PostgUrl      string
	PoolLimit     int16
	maxTaskWorker uint16
	taskQueueSize uint16
	done          chan int
	taskM         *manager.TaskManager
}

func NewTaskScheduler(done chan int, postgUrl string, poolLimit int16, maxTaskWorker uint16, taskQueueSize uint16) *TaskScheduler {
	return &TaskScheduler{
		done:          done,
		PostgUrl:      postgUrl,
		PoolLimit:     poolLimit,
		maxTaskWorker: maxTaskWorker,
		taskQueueSize: taskQueueSize,
	}
}

func (t *TaskScheduler) StartScheduler() error {
	postgClient, error := storage.NewPostgresClient(t.PostgUrl, t.PoolLimit)
	if error != nil {
		fmt.Printf("postgres cient failed %v\n", error)
		return error
	}
	ta := manager.NewTaskActor(t.maxTaskWorker, t.done, t.taskQueueSize)
	taskM := manager.InitManager(postgClient, ta, t.done)
	t.taskM = taskM
	taskM.StartManager()
	return nil
}

func (t *TaskScheduler) AddNewTask(task model.Task) error {
	t.taskM.AddNewTask(task)
	return nil
}
