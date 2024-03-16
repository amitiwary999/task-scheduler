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
	"strconv"
	"strings"
	"time"
	task "tskscheduler/task-scheduler/model"

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
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodPost, s.baseUrl, bytes.NewBuffer(data))
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
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("successfully insert %v\n", body)
	} else {
		log.Printf("error in insert with status %v\n", resp.StatusCode)
	}
	return id, nil
}

func (s *SupabaseClient) getTaskById(taskId string) ([]byte, int64, error) {
	url := fmt.Sprintf("%v?id=eq.%v&select=meta", s.baseUrl, taskId)
	req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if reqErr != nil {
		fmt.Printf("failed to create get task by id req %v\n", reqErr)
		return nil, 0, reqErr
	}
	authToken := fmt.Sprintf("Bearer %v", os.Getenv("SUPABASE_AUTH"))
	req.Header.Set("Authorization", authToken)
	req.Header.Set("apiKey", os.Getenv("SUPABASE_KEY"))
	resp, respErr := s.httpClinet.Do(req)
	if respErr != nil {
		return nil, 0, respErr
	}
	defer resp.Body.Close()
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return nil, 0, bodyErr
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("successfully insert %v\n", body)
	} else {
		log.Printf("error in insert with status %v\n", resp.StatusCode)
	}

	var count int64
	var err error

	contentRange := resp.Header.Get("Content-Range")
	if contentRange != "" {
		split := strings.Split(contentRange, "/")
		if len(split) > 1 && split[1] != "*" {
			count, err = strconv.ParseInt(split[1], 0, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("error parsing count from Content-Range header: %s", err.Error())
			}
		}
	}
	return body, count, nil
}
