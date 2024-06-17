package manager

type TaskActor struct {
	maxWorker   uint16
	workerCount uint16
	done        chan int
	taskChan    chan func()
	taskQueue   chan func()
}

func NewTaskActor(maxWorker uint16, done chan int, tasksSize uint16) *TaskActor {
	ta := &TaskActor{
		maxWorker:   maxWorker,
		workerCount: 0,
		done:        done,
		taskQueue:   make(chan func(), tasksSize),
		taskChan:    make(chan func()),
	}
	go ta.Dispatch()
	return ta
}

func (ta *TaskActor) SubmitTask(fn func()) {
	ta.taskChan <- fn
}

func (ta *TaskActor) Dispatch() {
	for {
		select {
		case taskF, ok := <-ta.taskChan:
			if !ok {
				break
			}
			if ta.workerCount < ta.maxWorker {
				go ta.DoAction(taskF)
				ta.workerCount++
			} else {
				ta.taskQueue <- taskF
			}
		case <-ta.done:
			return
		}
	}
}

func (ta *TaskActor) DoAction(fn func()) {
	task := fn
	for task != nil {
		task()
		task = <-ta.taskQueue
	}
	ta.workerCount--
}
