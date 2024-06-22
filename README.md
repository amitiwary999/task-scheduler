# TaskScheduler

This is use to complete task(function) in parllel to complete task fast. Each task consist of two things function that need to perform and metaId that use to fetch meta from the database using the metaId. Meta is use in the function to perform task. Each task(function) may or may not need parameters. 

Use go get github.com/amitiwary999/task-scheduler to fetch this module in your code.

import this library and then init the task scheduler

```
import ("github.com/amitiwary999/task-scheduler/scheduler")

tsk := scheduler.NewTaskScheduler(doneChannel, postgresUrl, poolLimit, workerCount, taskQueueLimit)
go tsk.StartScheduler()
meta := model.TaskMeta{
				MetaId: id,
			}
mdlTsk := model.Task{
    Meta:   meta,
    TaskFn: fn,
}
tsk.AddNewTask(mdlTsk)
```

When task is added it is added in taskQueue and worker fetch the task from this queue and perform it. Maximum workerCount number of worker use to perform task.