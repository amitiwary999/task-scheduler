package model

type ReceiveTask struct {
	ServerId string `json:"server"`
	TaskId   string `json:"task"`
}
