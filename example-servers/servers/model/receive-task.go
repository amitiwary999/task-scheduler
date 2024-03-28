package model

type ReceiveTask struct {
	ServerId string `json:"server"`
	TaskId   string `json:"task"`
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

type JoinData struct {
	ServerId string `json:"serverId"`
	Status   int    `json:"status"`
}

type UpdateServerStatus struct {
	Status int `json:"status"`
}
