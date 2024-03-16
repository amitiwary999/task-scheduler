package model

type TaskMeta struct {
	TaskId   string `json:"taskId"`
	TaskType string `json:"taskType"`
	MaxRetry int    `json:"maxRetry"`
	Action   string `json:"action"`
}

type Task struct {
	Id   string   `json:"id"`
	Meta TaskMeta `json:"meta"`
}

type Servers struct {
	Id   string `json:"id"`
	Load int    `json:"load"`
}

type TaskWeight struct {
	Type   string `json:"type"`
	Weight int    `json:"weight"`
}

type TaskMessage struct {
	ServerId string `json:"server"`
	TaskId   string `json:"task"`
}
