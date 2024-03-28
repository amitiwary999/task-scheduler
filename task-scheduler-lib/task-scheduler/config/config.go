package config

import (
	"encoding/json"
	"os"
	model "tskscheduler/task-scheduler/model"
)

type Config struct {
	TaskWeight []model.TaskWeight `json:"taskWeight"`
	Servers    []model.Servers    `json:"servers"`
}

func LoadConfig() *Config {
	configData, _ := os.ReadFile("./config.json")
	configS := string(configData)
	var config Config
	json.Unmarshal([]byte(configS), &config)
	return &config
}
