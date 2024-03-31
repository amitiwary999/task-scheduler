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
	"tskscheduler/task-scheduler/model"

	"github.com/google/uuid"
)

type SupabaseClient struct {
	httpClinet *http.Client
	baseUrl    string
}

func NewSupabaseClient() (*SupabaseClient, error) {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100
	client := &http.Client{
		Transport: t,
		Timeout:   60 * time.Second,
	}
	return &SupabaseClient{
		httpClinet: client,
		baseUrl:    os.Getenv("SUPABASE_JOBDETAIL_TABLE_URL"),
	}, nil
}
func (s *SupabaseClient) GetTaskConfig() ([]byte, error) {
	jobConfigTable := os.Getenv("SUPABASE_JOBCONFIG")
	url := fmt.Sprintf("%v%v", s.baseUrl, jobConfigTable)
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if reqErr != nil {
		fmt.Printf("failed to create get task by id req %v\n", reqErr)
		return nil, reqErr
	}
	authToken := fmt.Sprintf("Bearer %v", os.Getenv("SUPABASE_AUTH"))
	req.Header.Set("Authorization", authToken)
	req.Header.Set("apiKey", os.Getenv("SUPABASE_KEY"))
	resp, respErr := s.httpClinet.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	defer resp.Body.Close()
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return nil, bodyErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		log.Printf("successfully fetch all used server \n")
		return body, nil
	} else {
		log.Printf("error in fetch with status %v\n", resp.StatusCode)
		return nil, fmt.Errorf("failed to get the unused server")
	}
}

func (s *SupabaseClient) SaveTask(meta *model.TaskMeta) (string, error) {
	jobDetailTable := os.Getenv("SUPABASE_JOBDETAIL")
	url := fmt.Sprintf("%v%v", s.baseUrl, jobDetailTable)
	id := uuid.New().String()
	row := model.CompleteTask{
		Id:   id,
		Meta: *meta,
	}
	data, marshalErr := json.Marshal(row)
	if marshalErr != nil {
		return "", marshalErr
	}
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewBuffer(data))
	if reqErr != nil {
		return "", reqErr
	}
	authToken := fmt.Sprintf("Bearer %v", os.Getenv("SUPABASE_AUTH"))
	req.Header.Set("Authorization", authToken)
	req.Header.Set("apiKey", os.Getenv("SUPABASE_KEY"))
	resp, respErr := s.httpClinet.Do(req)
	if respErr != nil {
		return "", respErr
	}
	defer resp.Body.Close()
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return "", bodyErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		log.Printf("successfully insert %v\n", body)
	} else {
		log.Printf("error in insert with status %v\n", resp.StatusCode)
	}
	return id, nil
}

func (s *SupabaseClient) UpdateTaskComplete(id string) error {
	jobDetailTable := os.Getenv("SUPABASE_JOBDETAIL")
	updateS := model.TaskStatus{
		Status: "completed",
	}
	updateD, marshalErr := json.Marshal(updateS)
	if marshalErr != nil {
		fmt.Printf("marshal json for update task error %v\n", marshalErr)
		return marshalErr
	}
	url := fmt.Sprintf("%v%v?id=eq.%v", s.baseUrl, jobDetailTable, id)
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodPatch, url, bytes.NewBuffer(updateD))
	if reqErr != nil {
		fmt.Printf("req err creation err %v\n", reqErr)
	}
	authToken := fmt.Sprintf("Bearer %v", os.Getenv("SUPABASE_AUTH"))
	req.Header.Set("Authorization", authToken)
	req.Header.Set("apiKey", os.Getenv("SUPABASE_KEY"))
	resp, respErr := s.httpClinet.Do(req)
	if respErr != nil {
		return respErr
	}
	defer resp.Body.Close()
	_, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return bodyErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		log.Printf("successfully update \n")
		return nil
	} else {
		log.Printf("error in task status update %v\n", resp.StatusCode)
		return fmt.Errorf("error in task status update %v", resp.StatusCode)
	}
}

func (s *SupabaseClient) GetAllUsedServer() ([]byte, error) {
	jobServersTable := os.Getenv("SUPABASE_JOBSERVERS")
	url := fmt.Sprintf("%v%v?status=eq.1&select=serverId,status", s.baseUrl, jobServersTable)
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if reqErr != nil {
		fmt.Printf("failed to create get task by id req %v\n", reqErr)
		return nil, reqErr
	}
	authToken := fmt.Sprintf("Bearer %v", os.Getenv("SUPABASE_AUTH"))
	req.Header.Set("Authorization", authToken)
	req.Header.Set("apiKey", os.Getenv("SUPABASE_KEY"))
	resp, respErr := s.httpClinet.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	defer resp.Body.Close()
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return nil, bodyErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		log.Printf("successfully fetch all used server \n")
		return body, nil
	} else {
		log.Printf("error in fetch with status %v\n", resp.StatusCode)
		return nil, fmt.Errorf("failed to get the unused server")
	}
}
