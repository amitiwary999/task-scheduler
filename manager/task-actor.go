package manager

type TaskActor struct {
	workerCount uint16
	done        chan int
	taskQueue   chan struct{}
}
