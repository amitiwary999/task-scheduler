package manager

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	model "github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
)

type TaskManager struct {
	tasksWeight         map[string]model.TaskWeight
	servers             map[string]*model.Servers
	consumer            util.AMQPConsumer
	producer            util.AMQPProducer
	postgClient         util.PostgClient
	ReceiveTask         chan []byte
	ReceiveCompleteTask chan []byte
	serverJoin          chan []byte
	done                chan int
	lock                sync.Mutex
	priorityQueue       PriorityQueue
}

func InitManager(consumer util.AMQPConsumer, producer util.AMQPProducer, postgClient util.PostgClient, done chan int) *TaskManager {
	servers := make(map[string]*model.Servers)
	tasksWeight := make(map[string]model.TaskWeight)

	var taskWeightConfig []model.TaskWeight
	taskWeightConfig, _ = postgClient.GetTaskConfig()
	for _, taskWeight := range taskWeightConfig {
		tasksWeight[taskWeight.Type] = taskWeight
	}

	serversJoinData, serversErr := postgClient.GetAllUsedServer()

	if serversErr != nil {
		fmt.Printf("error in get all used servers %v\n", serversErr)
	} else {
		for _, serverJonData := range serversJoinData {
			server := model.Servers{
				Id:   serverJonData.ServerId,
				Load: 0,
			}
			servers[serverJonData.ServerId] = &server
		}
	}

	return &TaskManager{
		tasksWeight:         tasksWeight,
		servers:             servers,
		producer:            producer,
		consumer:            consumer,
		postgClient:         postgClient,
		ReceiveTask:         make(chan []byte),
		ReceiveCompleteTask: make(chan []byte),
		serverJoin:          make(chan []byte),
		done:                done,
		priorityQueue:       make(PriorityQueue, 0),
	}
}

func (tm *TaskManager) StartManager() {
	key := util.RABBITMQ_EXCHANGE_KEY
	queueName := util.RABBITMQ_TASK_QUEUE
	completeTaskKey := util.RABBITMQ_COMPLETE_TASK_EXCHANGE_KEY
	taskCompleteQueue := util.RABBITMQ_TASK_COMPLETE_QUEUE
	heap.Init(&tm.priorityQueue)
	go tm.consumer.Handle(tm.ReceiveTask, queueName, key, util.TaskConsumerTag)
	go tm.consumer.Handle(tm.ReceiveCompleteTask, taskCompleteQueue, completeTaskKey, util.CompleteTaskConsumerTag)
	go tm.consumer.ServerJoinHandle(tm.serverJoin, util.NewServerJoinTag)
	go tm.assignPendingTasks()
	go tm.delayTaskTicker()
}

func (tm *TaskManager) AddNewTask(task *model.Task) {
	if task.Meta.Delay > 0 {
		task.Meta.ExecutionTime = time.Now().Unix() + int64(task.Meta.Delay)*60
	}
	id, err := tm.postgClient.SaveTask(&task.Meta)
	if err != nil {
		fmt.Printf("failed to save the task %v\n", err)
	} else {
		task.Id = id
		if task.Meta.ExecutionTime > 0 {
			tm.priorityQueue.Push(&DelayTask{
				IdTask: task.Id,
				Time:   task.Meta.ExecutionTime,
			})
		} else {
			go tm.assignTask(id, task.Meta.TaskFn)
		}
	}
}

func (tm *TaskManager) assignTask(idTask string, taskFn func()) {
	fn := func() {
		taskFn()
		tm.postgClient.UpdateTaskComplete(idTask)
	}
}

func (tm *TaskManager) delayTaskTicker() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-tm.done:
			return
		case <-ticker.C:
			taskI := tm.priorityQueue.Pop()
			if taskI != nil {
				task := taskI.(*DelayTask)
				if task.Time-time.Now().Unix() <= 0 {
					go tm.assignTask(task.IdTask, task.TaskFn)
				} else {
					tm.priorityQueue.Push(task)
				}
			}
		}
	}
}

func (tm *TaskManager) assignPendingTasks() {
	pendingTasks, error := tm.postgClient.GetPendingTask()
	if error != nil {
		fmt.Printf("failed top fetch the pending tasks %v\n", error)
	} else {
	}
}
