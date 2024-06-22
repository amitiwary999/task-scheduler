package manager

import (
	"container/heap"
	"fmt"
	"time"

	model "github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
)

type TaskManager struct {
	postgClient   util.PostgClient
	taskActor     *TaskActor
	done          chan int
	priorityQueue PriorityQueue
}

func InitManager(postgClient util.PostgClient, taskActor *TaskActor, done chan int) *TaskManager {
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
		postgClient:   postgClient,
		taskActor:     taskActor,
		done:          done,
		priorityQueue: make(PriorityQueue, 0),
	}
}

func (tm *TaskManager) StartManager() {
	heap.Init(&tm.priorityQueue)
	go tm.delayTaskTicker()
}

func (tm *TaskManager) AddNewTask(task model.Task) {
	if task.Meta.Delay > 0 {
		task.Meta.ExecutionTime = time.Now().Unix() + int64(task.Meta.Delay)*60
	}
	id, err := tm.postgClient.SaveTask(&task.Meta)
	if err != nil {
		fmt.Printf("failed to save the task %v\n", err)
	} else {
		if task.Meta.ExecutionTime > 0 {
			tm.priorityQueue.Push(&DelayTask{
				IdTask: id,
				MetaId: task.Meta.MetaId,
				Time:   task.Meta.ExecutionTime,
			})
		} else {
			go tm.assignTask(id, task.Meta.MetaId, task.TaskFn)
		}
	}
}

func (tm *TaskManager) assignTask(idTask string, metaId string, taskFn func(metaId string)) {
	fn := func(metaId string) {
		taskFn(metaId)
		tm.postgClient.UpdateTaskComplete(idTask)
	}
	tsk := model.ActorTask{
		MetaId: metaId,
		TaskFn: fn,
	}
	tm.taskActor.SubmitTask(tsk)
}

func (tm *TaskManager) delayTaskTicker() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-tm.done:
			ticker.Stop()
			return
		case <-ticker.C:
			taskI := tm.priorityQueue.Pop()
			if taskI != nil {
				task := taskI.(*DelayTask)
				if task.Time-time.Now().Unix() <= 0 {
					go tm.assignTask(task.IdTask, task.MetaId, task.TaskFn)
				} else {
					tm.priorityQueue.Push(task)
				}
			}
		}
	}
}
