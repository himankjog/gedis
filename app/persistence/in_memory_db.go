package persistence

import (
	"fmt"
	"time"
)

type Value struct {
	Value          string
	ExpirationTime time.Time
	Expirable      bool
}

type SetOptions struct {
	ExpiryDuration time.Duration
}

var MEMORY_MAP map[string]Value

func init() {
	MEMORY_MAP = make(map[string]Value)
	go garbageCollector()
}

func Persist(key string, value string, options SetOptions) error {
	valueToPersist := Value{
		Value:     value,
		Expirable: false,
	}

	if options.ExpiryDuration > 0 {
		valueToPersist.Expirable = true
		valueToPersist.ExpirationTime = time.Now().Add(options.ExpiryDuration)
	}
	ctx.Logger.Printf("For key: %s persisting value: %s", key, value)
	MEMORY_MAP[key] = valueToPersist
	return nil
}

func Fetch(key string) (string, bool) {
	ctx.Logger.Printf("Fetching value for key: %s", key)
	value, valueExists := MEMORY_MAP[key]
	if !valueExists {
		ctx.Logger.Printf("No value found against key: %s", key)
		return "", false
	}

	isKeyExpired := (value.Expirable && time.Now().After(value.ExpirationTime))
	if !isKeyExpired {
		// If value exists and it hasn't expired, return the value
		ctx.Logger.Printf("Value fetched for key '%s' is: %s", key, value.Value)
		return value.Value, true
	}
	ctx.Logger.Printf("Value '%s' against  key '%s' has expired", value.Value, key)
	delete(MEMORY_MAP, key)
	return "", false
}

func garbageCollector() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		for key, val := range MEMORY_MAP {
			if val.Expirable && now.After(val.ExpirationTime) {
				delete(MEMORY_MAP, key)
				fmt.Println("Deleted expired key:", key)
			}
		}
	}
}
