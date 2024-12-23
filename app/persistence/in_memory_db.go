package persistence

import (
	"log"
	"time"
)

// TODO: Make it thread safe
var MEMORY_MAP map[string]string
var KEY_EXPIRY_MAP map[string]time.Time

func init() {
	MEMORY_MAP = make(map[string]string)
	KEY_EXPIRY_MAP = make(map[string]time.Time)
}

func Persist(key string, value string) error {
	log.Printf("For key: %s persisting value: %s", key, value)
	MEMORY_MAP[key] = value
	return nil
}

func Fetch(key string) (string, bool) {
	log.Printf("Fetching value for key: %s", key)
	value, valueExists := MEMORY_MAP[key]
	if !valueExists {
		log.Printf("No value found against key: %s", key)
		return "", false
	}

	expiryTime, expiryTimeExists := KEY_EXPIRY_MAP[key]
	isKeyExpired := (expiryTimeExists && time.Now().After(expiryTime))
	if !isKeyExpired {
		// If value exists and it hasn't expired, return the value
		log.Printf("Value fetched for key '%s' is: %s", key, value)
		return value, true
	}
	log.Printf("Value '%s' against  key '%s' has expired", value, key)
	delete(KEY_EXPIRY_MAP, key)
	delete(MEMORY_MAP, key)
	return "", false
}

func PersistWithExpiry(key string, value string, expiryDurationInMilli int) error {
	err := Persist(key, value)
	if err != nil {
		log.Printf("Error while trying to persist value '%s' for key '%s' with milliseconds expiry duration of '%d': %v", key, value, expiryDurationInMilli, err.Error())
		return err
	}
	keyExpiryTime := time.Now().Add(time.Duration(expiryDurationInMilli) * time.Millisecond)
	log.Printf("Added expiry time for key: '%s' as '%q'", key, keyExpiryTime)
	KEY_EXPIRY_MAP[key] = keyExpiryTime
	return err
}
