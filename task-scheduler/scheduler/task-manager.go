package scheduler

import (
	"log"
	qm "tskscheduler/task-scheduler/storage"
)

func InitManager(consumer *qm.Consumer, supClient *qm.SupabaseClient) {
	taskRecChan := make(chan string)
	receiveTask(taskRecChan, consumer.Done)
	go consumer.Handle(consumer.Delivery, consumer.Done, taskRecChan)
}

func receiveTask(recChan chan string, done chan int) {
	select {
	case <-done:
		return
	case <-recChan:
		for taskData := range recChan {
			log.Printf("task data %v\n", taskData)
		}
	}
}
