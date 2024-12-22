package persistence

import (
	"errors"
	"fmt"
	"log"
)

// TODO: Make it thread safe
var MEMORY_MAP map[string]string

func init() {
	MEMORY_MAP = make(map[string]string)
}

func Persist(key string, value string) error {
	log.Printf("For key: %s persisting value: %s", key, value)
	MEMORY_MAP[key] = value
	return nil
}

func Fetch(key string) (string, error) {
	log.Printf("Fetching value for key: %s", key)
	value, valueExists := MEMORY_MAP[key]

	if !valueExists {
		errMessage := fmt.Sprintf("[ValueNotFoundException] Value not found for key: %s", key)
		log.Println(errMessage)
		return "", errors.New(errMessage)
	}

	log.Printf("Value fetched for key '%s' is: %s", key, value)
	return value, nil
}
