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
		tsk := scheduler.NewTaskScheduler(done, os.Getenv("POSTGRES_URL"), int16(poolLimit), 10, 10000)
		err := tsk.StartScheduler()
		if err != nil {
			return
		}
		time.Sleep(time.Duration(time.Second * 3))
		for i := 0; i < 1000; i++ {
			fn := generateFunc()
			id := fmt.Sprintf("task_%v", i)
			meta := model.TaskMeta{
				MetaId: id,
			}
			mdlTsk := model.Task{
				Meta:   meta,
				TaskFn: fn,
			}
			tsk.AddNewTask(mdlTsk)
		}
		<-gracefulShutdown
		close(done)
	}
}
