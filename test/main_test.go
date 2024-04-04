package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	manag "github.com/amitiwary999/task-scheduler/manager"
	"github.com/amitiwary999/task-scheduler/model"

	"github.com/google/uuid"
)

type Consumer struct {
}

type Producer struct {
}

type SupabaseClient struct {
}

var cons = Consumer{}

var prod = Producer{}

var supa = SupabaseClient{}

func (c *Consumer) Shutdown() {

}

var taskM *manag.TaskManager

func (c *Consumer) Handle(data chan []byte, queueName string, key string, consumerTag string) error {
	return nil
}

func (c *Consumer) ServerJoinHandle(serverJoin chan []byte, consumerTag string) error {
	return nil
}

func (p *Producer) SendTaskMessage(taskId, routingKey string) {
	time.Sleep(time.Duration(time.Millisecond * 20))
	var meta = model.TaskMeta{
		TaskId:   "4nght45",
		TaskType: "task1",
		Action:   "COMPLETE_TASK",
		ServerId: "server1",
	}
	taskData := model.CompleteTask{
		Id:   taskId,
		Meta: meta,
	}
	taskDataByte, err := json.Marshal(taskData)
	if err != nil {
		fmt.Printf("error in send task msg %v\n", err)
	}
	taskM.ReceiveCompleteTask <- taskDataByte
}

func (p *Producer) Shutdown() {

}

func (s *SupabaseClient) GetTaskConfig() ([]byte, error) {
	var task1 = model.TaskWeight{
		Type:   "task1",
		Weight: 9,
	}

	var task2 = model.TaskWeight{
		Type:   "task2",
		Weight: 5,
	}

	var taskWeightConfig []model.TaskWeight
	taskWeightConfig = append(taskWeightConfig, task1)
	taskWeightConfig = append(taskWeightConfig, task2)
	return json.Marshal(taskWeightConfig)
}
func (s *SupabaseClient) SaveTask(meta *model.TaskMeta) (string, error) {
	id := uuid.New().String()
	time.Sleep(time.Duration(time.Millisecond) * 50)
	return id, nil
}
func (s *SupabaseClient) UpdateTaskComplete(id string) error {
	time.Sleep(time.Duration(time.Millisecond) * 50)
	return nil
}
func (s *SupabaseClient) GetAllUsedServer() ([]byte, error) {
	var joinData1 = model.JoinData{
		ServerId: "server1",
		Status:   0,
	}
	var joinData2 = model.JoinData{
		ServerId: "server2",
		Status:   0,
	}
	var serverConfig []model.JoinData
	serverConfig = append(serverConfig, joinData1)
	serverConfig = append(serverConfig, joinData2)
	mar, err := json.Marshal(serverConfig)
	if err != nil {
		return nil, err
	}
	return mar, nil
}

func (s *SupabaseClient) GetPendingTask() ([]byte, error) {
	return nil, nil
}

func BenchmarkTaskScheduler(b *testing.B) {
	done := make(chan int)
	taskM = manag.InitManager(&cons, &prod, &supa, done)
	taskM.StartManager()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			var meta = model.TaskMeta{
				TaskId:   fmt.Sprintf("45er3_%v", i),
				TaskType: "task2",
				Action:   "ADD_TASK",
			}
			var msg = model.Task{
				Meta: meta,
			}
			bdata, err := json.Marshal(msg)
			if err != nil {

			} else {
				taskM.ReceiveTask <- bdata
			}
		} else {
			var meta = model.TaskMeta{
				TaskId:   fmt.Sprintf("56dxrt_%v", i),
				TaskType: "task1",
				Action:   "ADD_TASK",
			}
			var msg = model.Task{
				Meta: meta,
			}
			bdata, err := json.Marshal(msg)
			if err != nil {

			} else {
				taskM.ReceiveTask <- bdata
			}
		}
	}
}
