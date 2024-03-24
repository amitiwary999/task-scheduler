package task

import (
	"encoding/json"
	"fmt"
	"os"
	model "tskscheduler/servers/model"
	util "tskscheduler/servers/util"
	qm "tskscheduler/task-scheduler/storage"
)

type cordinator struct {
	done      chan int
	consumer  *qm.Consumer
	producer  *qm.Producer
	supClient *qm.SupabaseClient
	receive   chan []byte
	serverId  string
}

func NewCordinator(consumer *qm.Consumer, producer *qm.Producer, supClient *qm.SupabaseClient, done chan int, serverId string) *cordinator {
	return &cordinator{
		done:      make(chan int),
		consumer:  consumer,
		producer:  producer,
		supClient: supClient,
		receive:   make(chan []byte),
		serverId:  serverId,
	}
}

func (c *cordinator) Start() {
	queueNamePrefix := os.Getenv("RABBITMQ_QUEUE_JOB_SERVER")
	queueName := fmt.Sprintf("%v_%v", queueNamePrefix, c.serverId)

	go c.consumer.Handle(c.receive, queueName, c.serverId)
	go c.receiveScheduledTask()
}

func (c *cordinator) receiveScheduledTask() {
	for {
		select {
		case <-c.done:
			return
		case task := <-c.receive:
			fmt.Printf("task in cordinator %v for server %v\n", task, c.serverId)
			var receiveTask model.ReceiveTask
			err := json.Unmarshal(task, &receiveTask)
			if err != nil {
				fmt.Printf("error in decoding receive task %v\n", err)
			} else {
				var taskData []model.Task
				taskDataByte, _, err := c.supClient.GetTaskById(receiveTask.TaskId)
				if err != nil {
					fmt.Printf("error in getting task data %v\n", err)
				}

				unMarshalErr := json.Unmarshal(taskDataByte, &taskData)
				if unMarshalErr != nil {
					fmt.Printf("un marshal error %v\n", unMarshalErr)
				} else {
					fmt.Printf("unmarshaled data %v\n", taskData)
					if len(taskData) > 0 {
						go c.doTask(taskData[0])
					}
				}
			}
		}
	}
}

func (c *cordinator) doTask(taskData model.Task) {
	taskType := taskData.Meta.TaskType
	if taskType == util.TASK_TYPE_1 {
		FirstTask()
	} else if taskType == util.TASK_TYPE_2 {
		SecondTask()
	}
	taskData.Meta.TaskId = c.serverId
	taskData.Meta.Action = "COMPLETE_TASK"
	c.producer.SendTaskCompleteMessage(&taskData)
}
