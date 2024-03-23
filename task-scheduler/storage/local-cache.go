package storage

import (
	"fmt"
	"os"
	"strconv"
	"time"

	cache "github.com/patrickmn/go-cache"
)

type LocalCache struct {
	cache *cache.Cache
}

func NewLocalCache() (*LocalCache, error) {
	expirationTime := os.Getenv("DEFAULT_EXPIRATION_LOCAL_CACHE")
	purgeTime := os.Getenv("PURGE_DURATION_LOCAL_CACHE")
	expirationTimeInt, expireConvErr := strconv.Atoi(expirationTime)

	if expireConvErr != nil {
		fmt.Printf("error in string to int conversion")
		return nil, expireConvErr
	}
	purgeTimeInt, purgeConvErr := strconv.Atoi(purgeTime)

	if purgeConvErr != nil {
		fmt.Printf("error in string to int conversion")
		return nil, purgeConvErr
	}

	cache := cache.New(time.Duration(expirationTimeInt)*time.Hour, time.Duration(purgeTimeInt)*time.Hour)
	return &LocalCache{
		cache: cache,
	}, nil
}
