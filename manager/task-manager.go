package manager

import (
	"container/heap"
	"fmt"
	"time"

	model "github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
)

type TaskManager struct {
	postgClient       util.PostgClient
	taskActor         *TaskActor
	done              chan int
	priorityQueue     PriorityQueue
	retryTimeDuration time.Duration
	funcGenerator     func() func(*model.TaskMeta) error
}

func InitManager(postgClient util.PostgClient, taskActor *TaskActor, retryTime time.Duration, funcGenerator func() func(*model.TaskMeta) error, done chan int) *TaskManager {
	return &TaskManager{
		postgClient:       postgClient,
		taskActor:         taskActor,
		funcGenerator:     funcGenerator,
		retryTimeDuration: retryTime,
		done:              done,
		priorityQueue:     make(PriorityQueue, 0),
	}
}

func (tm *TaskManager) StartManager() {
	heap.Init(&tm.priorityQueue)
	go tm.delayTaskTicker()
	go tm.retryFailedTask()
}

func (tm *TaskManager) AddNewTask(task model.Task) {
	if task.Meta.Delay > 0 {
		task.Meta.ExecutionTime = time.Now().Unix() + int64(task.Meta.Delay)*60
	}
	id, err := tm.postgClient.SaveTask(task.Meta)
	if err != nil {
		fmt.Printf("failed to save the task %v\n", err)
	} else {
		if task.Meta.ExecutionTime > 0 {
			tm.priorityQueue.Push(&model.DelayTask{
				IdTask: id,
				Meta:   task.Meta,
				Time:   task.Meta.ExecutionTime,
			})
		} else {
			go tm.assignTask(id, task.Meta)
		}
	}
}

func (tm *TaskManager) assignTask(idTask string, meta *model.TaskMeta) {
	fn := func(meta *model.TaskMeta) {
		err := tm.funcGenerator()(meta)
		taskStatus := util.JOB_DETAIL_STATUS_COMPLETED
		if err != nil {
			if meta.Retry <= 0 {
				return
			}
			meta.Retry = meta.Retry - 1
			taskStatus = util.JOB_DETAIL_STATUS_FAILED
		}
		tm.postgClient.UpdateTaskStatus(idTask, taskStatus, *meta)
	}
	tsk := model.ActorTask{
		Meta:   meta,
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
				task := taskI.(*model.DelayTask)
				if task.Time-time.Now().Unix() <= 0 {
					go tm.assignTask(task.IdTask, task.Meta)
				} else {
					tm.priorityQueue.Push(task)
				}
			}
		}
	}
}

func (tm *TaskManager) retryFailedTask() {
	ticker := time.NewTicker(tm.retryTimeDuration)
	for {
		select {
		case <-tm.done:
			ticker.Stop()
			return
		case <-ticker.C:
			tsks, err := tm.postgClient.GetFailTask()
			if err == nil {
				for _, tsk := range tsks {
					go tm.assignTask(tsk.Id, tsk.Meta)
				}
			} else {
				fmt.Printf("error to fetch failed task %v \n", err)
			}
		}
	}
}
