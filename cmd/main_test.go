package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/amitiwary999/task-scheduler/model"
	"github.com/amitiwary999/task-scheduler/scheduler"
	"github.com/joho/godotenv"
)

var wg sync.WaitGroup
var failedMap = make(map[string]int, 0)
var successC = 0
var failC = 0

func testGenerateFunc() func(*model.TaskMeta) error {
	return func(meta *model.TaskMeta) error {
		if meta.MetaId == "task_7" {
			if _, ok := failedMap[meta.MetaId]; !ok {
				failedMap[meta.MetaId] = 1
				wg.Done()
				failC++
			}
			return errors.New("task failed")
		}
		fmt.Printf("task with id %v completed \n", meta.MetaId)
		wg.Done()
		successC++
		return nil
	}
}

func TestTaskScheduler(t *testing.T) {
	err := godotenv.Load(".env")
	if err != nil {
		t.Errorf("error load env %v\n", err)
	}
	poolLimit, err := strconv.Atoi(os.Getenv("POSTGRES_POOL_LIMIT"))
	if err != nil {
		t.Errorf("error in the string conversion pool limit %v", err)
	} else {
		done := make(chan int)
		tconf := &scheduler.TaskConfig{
			PostgUrl:          os.Getenv("POSTGRES_URL"),
			PoolLimit:         int16(poolLimit),
			MaxTaskWorker:     10,
			TaskQueueSize:     10000,
			JobTableName:      "jobdetail",
			Done:              done,
			RetryTimeDuration: time.Duration(3 * time.Second),
			FuncGenerator:     testGenerateFunc,
		}
		tsk := scheduler.NewTaskScheduler(tconf)
		err := tsk.StartScheduler()
		if err != nil {
			return
		}
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("task_%v", i)
			meta := &model.TaskMeta{
				MetaId: id,
			}
			if id == "task_6" {
				meta.Retry = 5
			}
			mdlTsk := model.Task{
				Meta: meta,
			}
			err := tsk.AddNewTask(mdlTsk)
			if err == nil {
				wg.Add(1)
			}
		}
	}
	wg.Wait()
	if successC == 9 && failC == 1 {
		t.Log("successfully completed the tasks")
	} else {
		t.Errorf("fail to complete all the tasks expected %v found %v \n", 10, (successC + failC))
	}
}
