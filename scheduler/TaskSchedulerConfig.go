package scheduler

import (
	"time"

	manager "github.com/amitiwary999/task-scheduler/manager"
	model "github.com/amitiwary999/task-scheduler/model"
	"github.com/amitiwary999/task-scheduler/util"
)

type TaskConfig struct {
	MaxTaskWorker     uint16
	TaskQueueSize     uint16
	RetryTimeDuration time.Duration
	Done              chan int
	FuncGenerator     func() func(*model.TaskMeta) error
}

type TaskScheduler struct {
	maxTaskWorker     uint16
	taskQueueSize     uint16
	retryTimeDuration time.Duration
	funcGenerator     func() func(*model.TaskMeta) error
	done              chan int
	taskM             *manager.TaskManager
}

func NewTaskScheduler(tconf *TaskConfig) *TaskScheduler {
	return &TaskScheduler{
		done:              tconf.Done,
		maxTaskWorker:     tconf.MaxTaskWorker,
		taskQueueSize:     tconf.TaskQueueSize,
		funcGenerator:     tconf.FuncGenerator,
		retryTimeDuration: tconf.RetryTimeDuration,
	}
}

func (t *TaskScheduler) InitScheduler(storageClient util.StorageClient) {
	ta := manager.NewTaskActor(t.maxTaskWorker, t.done, t.taskQueueSize)
	taskM := manager.InitManager(storageClient, ta, t.retryTimeDuration, t.funcGenerator, t.done)
	t.taskM = taskM
	taskM.StartManager()
}

func (t *TaskScheduler) AddNewTask(task model.Task) error {
	return t.taskM.AddNewTask(task)
}
