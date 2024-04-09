# TaskScheduler

This can use to assign task to job server that basically do the job. This server use to schedule task based on load of the server. Each task belongs to a type and each type has weight that use to decide the load of the server based on number of task it perform.

RabbitMQ is use to communicate between task servers and this scheduler, supabase is use to save the job data.
Each job server has a unique id that it fetch from the JobServers table and this id is use to route task to correct server by the scheduler
JobConfig table has the task type and weight mapping.

Use go get github.com/amitiwary999/task-scheduler to fetch this module in your code.

import this library and then init the task scheduler

```
import ("github.com/amitiwary999/task-scheduler/scheduler")

tsk := scheduler.NewTaskScheduler("RABBITMQ_URL", "SUPABASE_API_BASE_URL", "SUPABASE_AUTH", "SUPABASE_KEY")
tsk.StartScheduler()
```

There is two way to send the task detail
1. add the data in rabbitmq for queue `tasks` and exchange key `sondesh`.
2. use AddNewTask function of TaskScheduler tsk.AddNewTask(byteArrayData). 
Json Data 
```
{
    "meta": {
        "taskId": "unoque if the task"
	    "taskType": "task type, it  should be one of type from the JobConfig table"
	    "maxRetry": "max number fo time retry for this task before finally failing"
	    "action":   "ADD_TASK"
    }
}
```
marshal this json and use it in `AddNewTask` function