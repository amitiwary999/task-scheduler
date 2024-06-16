package manager

type TaskActor struct {
	maxWorker uint16
	done      chan int
	taskChan  chan func()
	taskQueue chan func()
}

func NewTaskActor(workerCount uint16, done chan int, tasksSize uint16) *TaskActor {
	ta := &TaskActor{
		maxWorker: workerCount,
		done:      done,
		taskQueue: make(chan func(), tasksSize),
		taskChan:  make(chan func()),
	}
	go ta.Dispatch()
	return ta
}

func (ta *TaskActor) SubmitTask(fn func()) {
	ta.taskChan <- fn
}

func (ta *TaskActor) Dispatch() {
	var workerCount uint16 = 0
	for {
		select {
		case taskF, ok := <-ta.taskChan:
			if !ok {
				break
			}
			if workerCount < ta.maxWorker {
				go ta.DoAction()
			}
			ta.taskQueue <- taskF
		case <-ta.done:
			return
		}
	}
}

func (ta *TaskActor) DoAction() {

}
