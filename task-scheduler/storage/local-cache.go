package storage

import (
	"fmt"
	"os"
	"strconv"
	"time"

	util "tskscheduler/task-scheduler/util"

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

func (lc *LocalCache) AddNewServer(serverId string) {
	servrD, key := lc.cache.Get(util.LOCAL_CACHE_KEY_SERVER_JOIN)
	if key {
		serversId := servrD.([]string)
		serversId = append(serversId, serverId)
		lc.cache.Set(util.LOCAL_CACHE_KEY_SERVER_JOIN, serversId, 0)
	} else {
		lc.cache.Set(util.LOCAL_CACHE_KEY_SERVER_JOIN, []string{serverId}, 0)
	}
}

func (lc *LocalCache) RemoveServer(removedServerId string) {
	servrD, key := lc.cache.Get(util.LOCAL_CACHE_KEY_SERVER_JOIN)
	if key {
		serversId := servrD.([]string)
		updatedServerIds := serversId
		for i, serverId := range serversId {
			if removedServerId == serverId {
				updatedServerIds = append(serversId[:i], serversId[i+1:]...)
				break
			}
		}
		lc.cache.Set(util.LOCAL_CACHE_KEY_SERVER_JOIN, updatedServerIds, 0)
	}
}
