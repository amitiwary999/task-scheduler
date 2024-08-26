package main

import (
	"errors"
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

func generateFunc() func(*model.TaskMeta) error {
	return func(meta *model.TaskMeta) error {
		time.Sleep(time.Duration(time.Second * 1))
		if meta.MetaId == "task_700" {
			fmt.Printf("task with id %v failed \n", meta.MetaId)
			return errors.New("task failed")
		}
		fmt.Printf("task with id %v completed \n", meta.MetaId)
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
			PostgUrl:          os.Getenv("POSTGRES_URL"),
			PoolLimit:         int16(poolLimit),
			MaxTaskWorker:     10,
			TaskQueueSize:     10000,
			JobTableName:      "jobdetail",
			Done:              done,
			RetryTimeDuration: time.Duration(5 * time.Second),
			FuncGenerator:     generateFunc,
		}
		tsk := scheduler.NewTaskScheduler(tconf)
		err := tsk.StartScheduler()
		if err != nil {
			return
		}
		time.Sleep(time.Duration(time.Second * 3))
		for i := 0; i < 1; i++ {
			id := fmt.Sprintf("task_%v", i)
			meta := &model.TaskMeta{
				MetaId: id,
			}
			if id == "task_690" {
				meta.Retry = 5
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
