package task

import (
	"encoding/json"
	"fmt"
	model "tskscheduler/servers/model"
	qm "tskscheduler/storage"
)

type cordinator struct {
	done      chan int
	consumer  *qm.Consumer
	producer  *qm.Producer
	supClient *qm.SupabaseClient
	receive   chan []byte
}

func NewCordinator(consumer *qm.Consumer, producer *qm.Producer, supClient *qm.SupabaseClient, done chan int) *cordinator {
	return &cordinator{
		done:      make(chan int),
		consumer:  consumer,
		producer:  producer,
		supClient: supClient,
		receive:   make(chan []byte),
	}
}

func (c *cordinator) Start() {
	go c.consumer.Handle(c.receive)
	c.receiveScheduledTask()
}

func (c *cordinator) receiveScheduledTask() {
	select {
	case <-c.done:
		return
	case task := <-c.receive:
		fmt.Printf("task in cordinator %v\n", task)
		var receiveTask model.ReceiveTask
		err := json.Unmarshal(task, &receiveTask)
		if err != nil {
			fmt.Printf("error in decoding receive task %v\n", err)
		} else {

		}
	}
}
