package scheduler

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	manager "github.com/amitiwary999/task-scheduler/manager"
	model "github.com/amitiwary999/task-scheduler/model"
	storage "github.com/amitiwary999/task-scheduler/storage"
)

type TaskScheduler struct {
	RabbitmqUrl   string
	PostgUrl      string
	PoolLimit     int16
	maxTaskWorker uint16
	taskQueueSize uint16
	taskM         *manager.TaskManager
}

func NewTaskScheduler(rabbitmqUrl, postgUrl string, poolLimit int16, maxTaskWorker uint16, taskQueueSize uint16) *TaskScheduler {
	return &TaskScheduler{
		RabbitmqUrl:   rabbitmqUrl,
		PostgUrl:      postgUrl,
		PoolLimit:     poolLimit,
		maxTaskWorker: maxTaskWorker,
		taskQueueSize: taskQueueSize,
	}
}

func (t *TaskScheduler) StartScheduler() {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan int)
	postgClient, error := storage.NewPostgresClient(t.PostgUrl, t.PoolLimit)
	ta := manager.NewTaskActor(t.maxTaskWorker, done, t.taskQueueSize)
	if error != nil {
		fmt.Printf("postgres cient failed %v\n", error)
	}
	taskM := manager.InitManager(postgClient, ta, done)
	t.taskM = taskM
	taskM.StartManager()

	<-gracefulShutdown
	close(done)
}

func (t *TaskScheduler) AddNewTask(taskData []byte) error {
	var task model.Task
	err := json.Unmarshal(taskData, &task)
	if err != nil {
		return err
	} else {
		t.taskM.AddNewTask(&task)
		return nil
	}
}
