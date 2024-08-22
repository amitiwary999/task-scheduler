package scheduler

import (
	"fmt"

	manager "github.com/amitiwary999/task-scheduler/manager"
	model "github.com/amitiwary999/task-scheduler/model"
	storage "github.com/amitiwary999/task-scheduler/storage"
)

type TaskConfig struct {
	PostgUrl      string
	PoolLimit     int16
	MaxTaskWorker uint16
	TaskQueueSize uint16
	Done          chan int
	FuncGenerator func() func(string) error
}

type TaskScheduler struct {
	PostgUrl      string
	PoolLimit     int16
	maxTaskWorker uint16
	taskQueueSize uint16
	funcGenerator func() func(string) error
	done          chan int
	taskM         *manager.TaskManager
}

func NewTaskScheduler(tconf *TaskConfig) *TaskScheduler {
	return &TaskScheduler{
		done:          tconf.Done,
		PostgUrl:      tconf.PostgUrl,
		PoolLimit:     tconf.PoolLimit,
		maxTaskWorker: tconf.MaxTaskWorker,
		taskQueueSize: tconf.TaskQueueSize,
		funcGenerator: tconf.FuncGenerator,
	}
}

func (t *TaskScheduler) StartScheduler() error {
	postgClient, error := storage.NewPostgresClient(t.PostgUrl, t.PoolLimit)
	if error != nil {
		fmt.Printf("postgres cient failed %v\n", error)
		return error
	}
	ta := manager.NewTaskActor(t.maxTaskWorker, t.done, t.taskQueueSize)
	taskM := manager.InitManager(postgClient, ta, t.funcGenerator, t.done)
	t.taskM = taskM
	taskM.StartManager()
	return nil
}

func (t *TaskScheduler) AddNewTask(task model.Task) error {
	t.taskM.AddNewTask(task)
	return nil
}
