package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	serverModel "jobserver/servers/model"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
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

func (s *SupabaseClient) GetTaskById(taskId string) ([]byte, int64, error) {
	jobDetailTable := os.Getenv("SUPABASE_JOBDETAIL")
	url := fmt.Sprintf("%v%v?id=eq.%v&select=id,meta", s.baseUrl, jobDetailTable, taskId)
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
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		log.Printf("successfully fetch task detail\n")
	} else {
		log.Printf("error in fetch with status %v\n", resp.StatusCode)
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

func (s *SupabaseClient) GetUnusedServer() ([]byte, error) {
	jobServersTable := os.Getenv("SUPABASE_JOBSERVERS")
	url := fmt.Sprintf("%v%v?status=eq.0&limit=1&select=serverId,status", s.baseUrl, jobServersTable)
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
		log.Printf("successfully fetch one unused server\n")
		return body, nil
	} else {
		log.Printf("error in fetch with status %v\n", resp.StatusCode)
		return nil, fmt.Errorf("failed to get the unused server")
	}
}

func (s *SupabaseClient) UpdateServerStatus(serverId string, status int) error {
	jobServersTable := os.Getenv("SUPABASE_JOBSERVERS")
	updateS := serverModel.UpdateServerStatus{
		Status: status,
	}
	updateD, marshalErr := json.Marshal(updateS)
	if marshalErr != nil {
		fmt.Printf("marshal json for update task error %v\n", marshalErr)
		return marshalErr
	}
	url := fmt.Sprintf("%v%v?serverId=eq.%v", s.baseUrl, jobServersTable, serverId)
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
