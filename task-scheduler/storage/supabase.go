package storage

import (
	"log"
	task "tskscheduler/task-scheduler/model"

	"github.com/google/uuid"
	sb "github.com/nedpals/supabase-go"
)

type SupabaseClient struct {
	client *sb.Client
}

func NewSupabaseClient(url, key string) (*SupabaseClient, error) {
	client := sb.CreateClient(url, key)
	return &SupabaseClient{
		client: client,
	}, nil
}

func (s *SupabaseClient) SaveTask(meta *task.TaskMeta) (string, error) {
	id := uuid.New().String()
	row := task.Task{
		Id:   id,
		Meta: *meta,
	}
	var result []task.Task
	err := s.client.DB.From("JobDetail").Insert(row).Execute(&result)
	if err != nil {
		log.Printf("error in insert %v\n", err)
		return "", err
	} else {
		log.Printf("added data succ %v\n", result)
	}
	return id, nil
}
