package main

import (
	"fmt"
	"os"

	scheduler "github.com/amitiwary999/TaskScheduler/task-scheduler-lib/task-scheduler"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
	}
	tsk := scheduler.NewTaskScheduler(os.Getenv("RABBITMQ_URL"), os.Getenv("SUPABASE_AUTH"), os.Getenv("SUPABASE_KEY"))
	tsk.StartScheduler()
}
