package model

import "encoding/json"

type TaskMeta struct {
	MetaId        string `json:"metaId"`
	Delay         int    `json:"delay,omitempty"`
	ExecutionTime int64  `json:"executionTime,omitempty"`
	Retry         int    `json:"retry,omitempty"`
}

type Task struct {
	Meta *TaskMeta `json:"meta"`
	Id   string    `json:"id,omitempty"`
}

type PendingTask struct {
	Id   string    `json:"id"`
	Meta *TaskMeta `json:"meta"`
}

type DelayTask struct {
	IdTask string
	Meta   *TaskMeta
	Time   int64
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
	Meta   *TaskMeta
	TaskFn func(meta *TaskMeta)
}

type JoinData struct {
	ServerId string `json:"serverId"`
	Status   int    `json:"status"`
}

func (m *TaskMeta) Scan(value interface{}) error {
	type DefaultTskMeta TaskMeta
	defaultTskMeta := &DefaultTskMeta{
		Retry: 3,
	}
	if err := json.Unmarshal(value.([]byte), defaultTskMeta); err != nil {
		return err
	}
	*m = TaskMeta(*defaultTskMeta)
	return nil
}
