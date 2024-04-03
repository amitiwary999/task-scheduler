package util

import (
	"github.com/amitiwary999/task-scheduler/model"
)

type AMQPConsumer interface {
	Shutdown()
	Handle(data chan []byte, queueName string, key string, consumerTag string) error
	ServerJoinHandle(serverJoin chan []byte, consumerTag string) error
}

type AMQPProducer interface {
	Shutdown()
	SendTaskMessage(taskId, routingKey string)
}

type SupabaseClient interface {
	SaveTask(meta *model.TaskMeta) (string, error)
	UpdateTaskComplete(id string) error
	GetAllUsedServer() ([]byte, error)
	GetTaskConfig() ([]byte, error)
}
