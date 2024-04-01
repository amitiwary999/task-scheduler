package taskscheduler

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	manager "tskscheduler/task-scheduler/scheduler"
	storage "tskscheduler/task-scheduler/storage"
	util "tskscheduler/task-scheduler/util"
)

type TaskScheduler struct {
	RabbitmqUrl  string
	SupabaseAuth string
	SupabaseKey  string
}

func NewTaskScheduler(rabbitmqUrl, supabaseAuth, supabaseKey string) *TaskScheduler {
	return &TaskScheduler{
		RabbitmqUrl:  rabbitmqUrl,
		SupabaseAuth: supabaseAuth,
		SupabaseKey:  supabaseKey,
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
	supa, error := storage.NewSupabaseClient(t.SupabaseAuth, t.SupabaseKey)
	if error != nil {
		fmt.Printf("supabase cloient failed %v\n", error)
	}
	taskM := manager.InitManager(consumer, producer, supa, done)
	taskM.StartManager()

	<-gracefulShutdown
	close(done)
	consumer.Shutdown()
	producer.Shutdown()
}
