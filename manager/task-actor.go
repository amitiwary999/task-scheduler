package manager

import (
	model "github.com/amitiwary999/task-scheduler/model"
)

type TaskActor struct {
	maxWorker uint16
	done      chan int
	taskChan  chan model.ActorTask
	taskQueue chan model.ActorTask
}

func NewTaskActor(maxWorker uint16, done chan int, tasksSize uint16) *TaskActor {
	ta := &TaskActor{
		maxWorker: maxWorker,
		done:      done,
		taskQueue: make(chan model.ActorTask, tasksSize),
		taskChan:  make(chan model.ActorTask),
	}
	go ta.Dispatch()
	return ta
}

func (ta *TaskActor) SubmitTask(tsk model.ActorTask) {
	ta.taskChan <- tsk
}

func (ta *TaskActor) Dispatch() {
	var workerCount uint16 = 0
ExitLoop:
	for {
		select {
		case taskF, ok := <-ta.taskChan:
			if !ok {
				break
			}
			if workerCount < ta.maxWorker {
				go ta.DoAction(taskF)
				workerCount++
			} else {
				ta.taskQueue <- taskF
			}
		case <-ta.done:
			break ExitLoop
		}
	}
}

func (ta *TaskActor) DoAction(tsk model.ActorTask) {
	task := tsk
ExitLoop:
	for {
		task.TaskFn(task.MetaId)
		select {
		case task = <-ta.taskQueue:
		case <-ta.done:
			break ExitLoop
		}
	}
}
