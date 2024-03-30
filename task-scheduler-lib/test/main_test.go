package main

import (
	"encoding/json"
	"fmt"
	"testing"
	cnfg "tskscheduler/task-scheduler/config"
	"tskscheduler/task-scheduler/model"
	manag "tskscheduler/task-scheduler/scheduler"

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

var joinData1 = model.JoinData{
	ServerId: "server1",
	Status:   0,
}

func (c *Consumer) Handle(data chan []byte, queueName string, key string, consumerTag string) error {
	return nil
}

func (c *Consumer) ServerJoinHandle(serverJoin chan []byte, consumerTag string) error {
	fmt.Printf("receive req for server join%v\n", consumerTag)
	return nil
}

func (p *Producer) SendTaskMessage(taskId, routingKey string) {
	// time.Sleep(time.Duration(time.Millisecond * 60))
	var meta = model.TaskMeta{
		TaskId:   "4nght45",
		TaskType: "task1",
		Action:   "COMPLETE_TASK",
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

func (s *SupabaseClient) SaveTask(meta *model.TaskMeta) (string, error) {
	id := uuid.New().String()
	// time.Sleep(time.Duration(time.Millisecond) * 100)
	return id, nil
}
func (s *SupabaseClient) UpdateTaskComplete(id string) error {
	// time.Sleep(time.Duration(time.Millisecond) * 100)
	fmt.Printf("update task complete for %v\n", id)
	return nil
}
func (s *SupabaseClient) GetAllUsedServer() ([]byte, error) {
	mar, err := json.Marshal(joinData1)
	if err != nil {
		return nil, err
	}
	return mar, nil
}

func BenchmarkTaskScheduler(b *testing.B) {
	done := make(chan int)
	data := make(chan []byte)
	taskM = manag.InitManager(&cons, &prod, &supa, done, cnfg.LoadConfig())
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
				data <- bdata
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
