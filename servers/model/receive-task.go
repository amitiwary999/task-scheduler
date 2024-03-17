package model

type ReceiveTask struct {
	ServerId string `json:"server"`
	TaskId   string `json:"task"`
}

type CompleteTask struct {
	TaskId   string `json:"taskId"`
	ServerId string `json:"serverId"`
}

type TaskMeta struct {
	TaskId   string `json:"taskId"`
	TaskType string `json:"taskType"`
	Action   string `json:"action"`
}

type Task struct {
	Id   string   `json:"id"`
	Meta TaskMeta `json:"meta"`
}
