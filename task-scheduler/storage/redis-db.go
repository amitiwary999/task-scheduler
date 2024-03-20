package storage

import (
	"fmt"
	"os"

	redis "github.com/go-redis/redis"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient() (*RedisClient, error) {
	addr := os.Getenv("REDIS_URL")
	pswd := os.Getenv("REDIS_PASSWORD")
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pswd,
	})
	pong, err := client.Ping().Result()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		return nil, err
	}
	fmt.Println("Connected to Redis:", pong)

	return &RedisClient{
		client: client,
	}, nil
}
