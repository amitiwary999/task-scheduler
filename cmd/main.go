package main

import (
	"fmt"
	"os"

	scheduler "github.com/amitiwary999/task-scheduler/scheduler"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("error load env %v\n", err)
	}
	tsk := scheduler.NewTaskScheduler(os.Getenv("RABBITMQ_URL"), os.Getenv("SUPABASE_API_BASE_URL"), os.Getenv("SUPABASE_AUTH"), os.Getenv("SUPABASE_KEY"))
	tsk.StartScheduler()
}
