package persistence

import (
	"testing"
	"time"
)

func TestPersist(t *testing.T) {
	MEMORY_MAP = make(map[string]Value) // Reset map before each test

	// Test persisting a value without expiry
	err := Persist("key1", "value1", SetOptions{})
	if err != nil {
		t.Errorf("Persisting without expiry failed: %v", err)
	}

	// Check if value is stored in the map
	value, exists := MEMORY_MAP["key1"]
	if !exists {
		t.Errorf("Value not found in map after persisting")
	}
	if value.Expirable {
		t.Errorf("Value should not be marked as expirable")
	}

	// Test persisting a value with expiry
	err = Persist("key2", "value2", SetOptions{ExpiryDuration: 100 * time.Millisecond})
	if err != nil {
		t.Errorf("Persisting with expiry failed: %v", err)
	}

	// Check if value is stored in the map and marked as expirable
	value, exists = MEMORY_MAP["key2"]
	if !exists {
		t.Errorf("Value not found in map after persisting with expiry")
	}
	if !value.Expirable {
		t.Errorf("Value should be marked as expirable")
	}
}

func TestFetch(t *testing.T) {
	MEMORY_MAP = make(map[string]Value) // Reset map before each test

	// Test fetching a non-existent key
	value, exists := Fetch("non-existent-key")
	if exists {
		t.Errorf("Fetching non-existent key should return false")
	}

	// Persist a value without expiry
	err := Persist("key1", "value1", SetOptions{})
	if err != nil {
		t.Errorf("Persisting failed: %v", err)
	}

	// Test fetching an existing key without expiry
	value, exists = Fetch("key1")
	if !exists {
		t.Errorf("Fetching existing key should return true")
	}
	if value != "value1" {
		t.Errorf("Fetched value doesn't match persisted value")
	}

	// Persist a value with expiry
	err = Persist("key2", "value2", SetOptions{ExpiryDuration: 1 * time.Second})
	if err != nil {
		t.Errorf("Persisting with expiry failed: %v", err)
	}

	// Sleep for a little longer than the expiry duration
	time.Sleep(2 * time.Second)

	// Test fetching an expired key
	_, exists = Fetch("key2")
	if exists {
		t.Errorf("Fetching expired key should return false")
	}

	// Check if the key is removed from the map after expiration
	_, exists = MEMORY_MAP["key2"]
	if exists {
		t.Errorf("Expired key should be removed from map")
	}
}
