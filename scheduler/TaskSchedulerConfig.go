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
	util "github.com/amitiwary999/task-scheduler/util"
)

type TaskScheduler struct {
	RabbitmqUrl string
	PostgUrl    string
	PoolLimit   int16
	taskM       *manager.TaskManager
}

func NewTaskScheduler(rabbitmqUrl, postgUrl string, poolLimit int16) *TaskScheduler {
	return &TaskScheduler{
		RabbitmqUrl: rabbitmqUrl,
		PostgUrl:    postgUrl,
		PoolLimit:   poolLimit,
	}
}

func (t *TaskScheduler) StartScheduler() {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan int)
	producerQueueName := util.RABBITMQ_QUEUE_JOB_SERVER
	consumer, err := storage.NewConsumer(done, t.RabbitmqUrl)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	}
	producer, err := storage.NewProducer(done, producerQueueName, t.RabbitmqUrl)
	if err != nil {
		fmt.Printf("amq connection error %v\n", err)
	}
	postgClient, error := storage.NewPostgresClient(t.PostgUrl, t.PoolLimit)
	if error != nil {
		fmt.Printf("supabase cloient failed %v\n", error)
	}
	taskM := manager.InitManager(consumer, producer, postgClient, done)
	t.taskM = taskM
	taskM.StartManager()

	<-gracefulShutdown
	close(done)
	consumer.Shutdown()
	producer.Shutdown()
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
