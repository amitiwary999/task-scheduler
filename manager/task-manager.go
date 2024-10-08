package manager

import (
	"container/heap"
	"fmt"
	"time"

	model "github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
)

type TaskManager struct {
	storageClient     util.StorageClient
	taskActor         *TaskActor
	done              chan int
	priorityQueue     PriorityQueue
	retryTimeDuration time.Duration
	funcGenerator     func() func(*model.TaskMeta) error
}

func InitManager(storageClient util.StorageClient, taskActor *TaskActor, retryTime time.Duration, funcGenerator func() func(*model.TaskMeta) error, done chan int) *TaskManager {
	return &TaskManager{
		storageClient:     storageClient,
		taskActor:         taskActor,
		funcGenerator:     funcGenerator,
		retryTimeDuration: retryTime,
		done:              done,
		priorityQueue:     make(PriorityQueue, 0),
	}
}

func (tm *TaskManager) StartManager() {
	heap.Init(&tm.priorityQueue)
	tm.loadPendingTask()
	go tm.delayTaskTicker()
	go tm.retryFailedTask()
}

func (tm *TaskManager) AddNewTask(task model.Task) error {
	if task.Meta.Delay > 0 {
		task.Meta.ExecutionTime = time.Now().Unix() + int64(task.Meta.Delay)*60
	}
	id, err := tm.storageClient.SaveTask(task.Meta)
	if err != nil {
		fmt.Printf("failed to save the task %v\n", err)
		return err
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
	return nil
}

func (tm *TaskManager) assignTask(idTask string, meta *model.TaskMeta) {
	fn := func(meta *model.TaskMeta) {
		err := tm.funcGenerator()(meta)
		taskStatus := util.JOB_DETAIL_STATUS_COMPLETED
		if err != nil {
			meta.Retry = meta.Retry - 1
			if meta.Retry <= 0 {
				taskStatus = util.JOB_DETAIL_STATUS_DEAD
			} else {
				taskStatus = util.JOB_DETAIL_STATUS_FAILED
			}
		}
		tm.storageClient.UpdateTaskStatus(idTask, taskStatus, *meta)
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
			tsks, err := tm.storageClient.GetFailTask()
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

func (tm *TaskManager) loadPendingTask() {
	tsks, err := tm.storageClient.GetPendingTask()
	if err == nil {
		for _, tsk := range tsks {
			go tm.assignTask(tsk.Id, tsk.Meta)
		}
	} else {
		fmt.Printf("failed to fetch pending tasks %v \n", err)
	}
}
