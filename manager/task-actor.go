package manager

import (
	"time"

	model "github.com/amitiwary999/task-scheduler/model"
)

var stopTaskActor bool = true

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
	stopTaskActor = false
	ta.taskChan <- tsk
}

func (ta *TaskActor) Dispatch() {
	ticker := time.NewTicker(10 * time.Second)
	var workerCount uint16 = 0
ExitLoop:
	for {
		select {
		case taskF, ok := <-ta.taskChan:
			if !ok {
				break ExitLoop
			}
			if workerCount < ta.maxWorker {
				go ta.DoAction(taskF)
				workerCount++
			} else {
				ta.taskQueue <- taskF
			}
		case <-ticker.C:
			if stopTaskActor {
				actr := model.ActorTask{
					MetaId: "",
					TaskFn: nil,
				}
				for workerCount > 0 {
					ta.taskQueue <- actr
					workerCount--
				}
				ticker.Stop()
			}
			stopTaskActor = true
		case <-ta.done:
			break ExitLoop
		}
	}
}

func (ta *TaskActor) DoAction(tsk model.ActorTask) {
	task := tsk
ExitLoop:
	for {
		if task.TaskFn == nil {
			break ExitLoop
		}
		task.TaskFn(task.MetaId)
		select {
		case task = <-ta.taskQueue:
		case <-ta.done:
			break ExitLoop
		}
	}
}
