package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
	task "tskscheduler/task-scheduler/model"

	"github.com/google/uuid"
)

type SupabaseClient struct {
}

func NewSupabaseClient() (*SupabaseClient, error) {
	return &SupabaseClient{}, nil
}

func (s *SupabaseClient) SaveTask(meta *task.TaskMeta) (string, error) {
	id := uuid.New().String()
	row := task.Task{
		Id:   id,
		Meta: *meta,
	}
	data, marshalErr := json.Marshal(row)
	if marshalErr != nil {
		return "", marshalErr
	}
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodPost, os.Getenv("SUPABASE_JOBDETAIL_TABLE_URL"), bytes.NewBuffer(data))
	if reqErr != nil {
		return "", reqErr
	}
	authToken := fmt.Sprintf("Bearer %v", os.Getenv("SUPABASE_AUTH"))
	req.Header.Set("Authorization", authToken)
	req.Header.Set("apiKey", os.Getenv("SUPABASE_KEY"))
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100
	client := &http.Client{
		Transport: t,
		Timeout:   60 * time.Second,
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return "", respErr
	}
	defer resp.Body.Close()
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return "", bodyErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("successfully insert %v\n", body)
	} else {
		log.Printf("error in insert with status %v\n", resp.StatusCode)
	}
	return id, nil
}
