package model

import "encoding/json"

type TaskMeta struct {
	MetaId        string `json:"metaId"`
	Delay         int    `json:"delay,omitempty"`
	ExecutionTime int64  `json:"executionTime,omitempty"`
}

type Task struct {
	Meta TaskMeta `json:"meta"`
	Id   string   `json:"id,omitempty"`
}

type CompleteTask struct {
	Id   string   `json:"id"`
	Meta TaskMeta `json:"meta"`
}

type PendingTask struct {
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

type TaskStatus struct {
	Status string `json:"status"`
}

type ActorTask struct {
	MetaId string
	TaskFn func(metaId string)
}

type JoinData struct {
	ServerId string `json:"serverId"`
	Status   int    `json:"status"`
}

func (m *TaskMeta) Scan(value interface{}) error {
	return json.Unmarshal(value.([]byte), m)
}
