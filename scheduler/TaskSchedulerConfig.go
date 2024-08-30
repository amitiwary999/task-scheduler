package scheduler

import (
	"fmt"
	"time"

	manager "github.com/amitiwary999/task-scheduler/manager"
	model "github.com/amitiwary999/task-scheduler/model"
	storage "github.com/amitiwary999/task-scheduler/storage"
	"github.com/amitiwary999/task-scheduler/util"
)

type TaskConfig struct {
	PostgUrl          string
	PoolLimit         int16
	JobTableName      string
	MaxTaskWorker     uint16
	TaskQueueSize     uint16
	RetryTimeDuration time.Duration
	Done              chan int
	FuncGenerator     func() func(*model.TaskMeta) error
}

type TaskScheduler struct {
	postgUrl          string
	poolLimit         int16
	maxTaskWorker     uint16
	taskQueueSize     uint16
	jobTableName      string
	retryTimeDuration time.Duration
	funcGenerator     func() func(*model.TaskMeta) error
	done              chan int
	taskM             *manager.TaskManager
}

func NewTaskScheduler(tconf *TaskConfig) *TaskScheduler {
	return &TaskScheduler{
		done:              tconf.Done,
		postgUrl:          tconf.PostgUrl,
		poolLimit:         tconf.PoolLimit,
		maxTaskWorker:     tconf.MaxTaskWorker,
		taskQueueSize:     tconf.TaskQueueSize,
		jobTableName:      tconf.JobTableName,
		funcGenerator:     tconf.FuncGenerator,
		retryTimeDuration: tconf.RetryTimeDuration,
	}
}

func (t *TaskScheduler) InitStorage() (util.PostgClient, error) {
	postgClient, error := storage.NewPostgresClient(t.postgUrl, t.poolLimit, t.jobTableName)
	if error != nil {
		fmt.Printf("postgres cient failed %v\n", error)
		return nil, error
	}
	err := postgClient.CreateJobTable()
	if err != nil {
		fmt.Printf("failed to create the table to save job details %v \n", err)
		return nil, err
	}
	return postgClient, nil
}

func (t *TaskScheduler) InitScheduler(postgClient util.PostgClient) {
	ta := manager.NewTaskActor(t.maxTaskWorker, t.done, t.taskQueueSize)
	taskM := manager.InitManager(postgClient, ta, t.retryTimeDuration, t.funcGenerator, t.done)
	t.taskM = taskM
	taskM.StartManager()
}

func (t *TaskScheduler) AddNewTask(task model.Task) error {
	return t.taskM.AddNewTask(task)
}
