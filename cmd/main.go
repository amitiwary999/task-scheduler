package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	model "github.com/amitiwary999/task-scheduler/model"
	scheduler "github.com/amitiwary999/task-scheduler/scheduler"

	"github.com/joho/godotenv"
)

func generateFunc() func(string) error {
	return func(metaId string) error {
		time.Sleep(time.Duration(time.Second * 1))
		fmt.Printf("task with id %v completed \n", metaId)
		return nil
	}
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
	}
	poolLimit, err := strconv.Atoi(os.Getenv("POSTGRES_POOL_LIMIT"))
	if err != nil {
		fmt.Printf("error in the string conversion pool limit %v", err)
	} else {
		done := make(chan int)
		gracefulShutdown := make(chan os.Signal, 1)
		signal.Notify(gracefulShutdown, syscall.SIGINT, syscall.SIGTERM)
		tconf := &scheduler.TaskConfig{
			PostgUrl:      os.Getenv("POSTGRES_URL"),
			PoolLimit:     int16(poolLimit),
			MaxTaskWorker: 10,
			TaskQueueSize: 10000,
			JobTableName:  "jobdetail",
			Done:          done,
			FuncGenerator: generateFunc,
		}
		tsk := scheduler.NewTaskScheduler(tconf)
		err := tsk.StartScheduler()
		if err != nil {
			return
		}
		time.Sleep(time.Duration(time.Second * 3))
		for i := 0; i < 1000; i++ {
			id := fmt.Sprintf("task_%v", i)
			meta := model.TaskMeta{
				MetaId: id,
			}
			mdlTsk := model.Task{
				Meta: meta,
			}
			tsk.AddNewTask(mdlTsk)
		}
		<-gracefulShutdown
		close(done)
	}
}
