package main

import (
	"fmt"
	"os"
	"strconv"

	scheduler "github.com/amitiwary999/task-scheduler/scheduler"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
	}
	poolLimit, err := strconv.Atoi(os.Getenv("POSTGRES_POOL_LIMIT"))
	if err != nil {
		fmt.Printf("error in the string conversion pool limit %v", err)
	} else {
		tsk := scheduler.NewTaskScheduler(os.Getenv("RABBITMQ_URL"), os.Getenv("POSTGRES_URL"), int16(poolLimit))
		tsk.StartScheduler()
	}
}
