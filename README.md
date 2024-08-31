# TaskScheduler
This can use to peform tasks, with option to retry if failed and also peform task after some delay, in parallel to complete task quickly. Each task consist of two things function that need to perform and metaId that use to fetch meta from the database using the metaId. Meta is use in the function to perform task. Each task(function) may or may not need parameters. 

Use go get github.com/amitiwary999/task-scheduler to fetch this module in your code.

import this library and then init the task scheduler

```
import ("github.com/amitiwary999/task-scheduler/scheduler")

tconf := &scheduler.TaskConfig{
    MaxTaskWorker:     10,
    TaskQueueSize:     10000,
    Done:              done,
    RetryTimeDuration: time.Duration(5 * time.Second),
    FuncGenerator:     generateFunc,
}
tsk := scheduler.NewTaskScheduler(tconf)
/**
storage use to save the task so that we can fetch fail task later to retry again. Scheduler need StorageClient interface so that it can perform the storage operation. So before InitScheduler call make sure to get a StorageClient interface. We provide the function to create postgres db client. Will add support for the other storage later.
*/
dbClient, err := storage.NewPostgresClient(os.Getenv("POSTGRES_URL"), int16(poolLimit), "jobdetail")
tsk.InitScheduler(dbClient)

/** 
After init scheduler successfully, Task can be submit. Each task has Meta of task and TaskFn function to perform the task.
*/
meta := model.TaskMeta{
	MetaId: id,
    Delay: intValue(after how much delay task need to perform, optional)
    ExecutionTime: intValue(at what time need to perform task, optional)
}
mdlTsk := model.Task{
    Meta:   meta,
    TaskFn: fn,
}
tsk.AddNewTask(mdlTsk)
```

When task is added it is added in taskQueue and worker fetch the task from this queue and perform it. Maximum workerCount number of worker use to perform task.
Postgres is use to save the task (metaId, delay, execution time, status) and once task is complete status is changed to complete. This helps if an assigned task is not performed successfully then on next server start fetch the task from database and add it to queue.