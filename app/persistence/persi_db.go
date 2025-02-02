package persistence

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/constants"
	"github.com/codecrafters-io/redis-starter-go/app/context"
)

type ValueType byte

// Value Types
const (
	STRING ValueType = 0x00
	LIST   ValueType = 0x01
	SET    ValueType = 0x02
	ZSET   ValueType = 0x03
	HASH   ValueType = 0x04
	ZIPMAP ValueType = 0x09
)

func (vt ValueType) String() string {
	switch vt {
	case STRING:
		return "string"
	case LIST:
		return "list"
	case SET:
		return "set"
	case ZSET:
		return "zset"
	case HASH:
		return "hash"
	case ZIPMAP:
		return "zipmap"
	default:
		return constants.NONE
	}
}

type Value struct {
	Data           []byte
	Type           string
	ExpirationTime *time.Time
}

type SetOptions struct {
	ExpiryDuration time.Duration
	ValueType      string
}

type Memory struct {
	memoryMap           map[string]Value
	expirableMemoryMap  map[string]Value
	lock                *sync.RWMutex
	expirableMemoryLock *sync.RWMutex
}

func initMemory() *Memory {
	lock := sync.RWMutex{}
	expirableMemoryLock := sync.RWMutex{}
	memoryMap := make(map[string]Value, 0)
	expirableMemoryMap := make(map[string]Value, 0)

	return &Memory{
		memoryMap:           memoryMap,
		expirableMemoryMap:  expirableMemoryMap,
		lock:                &lock,
		expirableMemoryLock: &expirableMemoryLock,
	}
}

func (mem *Memory) Get(key string) (Value, bool) {
	mem.lock.RLock()
	value, valueExists := mem.memoryMap[key]
	if !valueExists {
		mem.lock.RUnlock()
		mem.expirableMemoryLock.RLock()
		defer mem.expirableMemoryLock.RUnlock()
		value, valueExists = mem.expirableMemoryMap[key]
		return value, valueExists
	}
	mem.lock.RUnlock()
	return value, valueExists

}

func (mem *Memory) Set(key string, val Value) error {
	if val.ExpirationTime != nil {
		mem.expirableMemoryLock.Lock()
		defer mem.expirableMemoryLock.Unlock()
		mem.expirableMemoryMap[key] = val
		return nil
	}
	mem.lock.Lock()
	defer mem.lock.Unlock()
	mem.memoryMap[key] = val

	return nil
}

func (mem *Memory) Delete(key string) (Value, bool) {
	mem.lock.Lock()
	defer mem.lock.Unlock()
	value, valueExists := mem.memoryMap[key]
	delete(mem.memoryMap, key)

	return value, valueExists
}

func (mem *Memory) DeleteExpired(key string) (Value, bool) {
	mem.expirableMemoryLock.Lock()
	defer mem.expirableMemoryLock.Unlock()
	value, valueExists := mem.expirableMemoryMap[key]
	delete(mem.expirableMemoryMap, key)

	return value, valueExists
}

func (mem *Memory) GetAllKeys() []string {
	keys := make([]string, 0)
	mem.lock.RLock()
	defer mem.lock.RUnlock()
	for key := range mem.memoryMap {
		keys = append(keys, key)
	}
	now := time.Now()
	for key, val := range mem.GetAllExpirableKeyValuePair() {
		if val.ExpirationTime == nil || now.Before(*val.ExpirationTime) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (mem *Memory) GetAllExpirableKeyValuePair() map[string]Value {
	mem.expirableMemoryLock.RLock()
	defer mem.expirableMemoryLock.RUnlock()
	return mem.expirableMemoryMap
}

type PersiDb struct {
	ctx        *context.Context
	logger     *log.Logger
	dbDir      string
	dbFileName string
	Memory     *Memory
	streamMap  map[string]*Stream
}

func (db *PersiDb) load() {
	if len(db.dbDir) == 0 || len(db.dbFileName) == 0 {
		return
	}
	rdbFilePath := db.dbDir + "/" + db.dbFileName
	_, err := os.Stat(rdbFilePath)
	if err != nil {
		db.logger.Printf("RDB file not found at path: %s", rdbFilePath)
		return
	}
	loadedRdb, err := LoadRDB(rdbFilePath, db.logger)
	if err != nil {
		db.logger.Printf("Error while reading RDB file at path: %s, error: %v", rdbFilePath, err)
		return
	}
	db.logger.Printf("Successfully loaded RDB file from path: %s", rdbFilePath)
	for dbIndex, database := range loadedRdb.dbs {
		fmt.Printf("Loading database: %d\n", dbIndex)
		for key, value := range database.nonExpirableData {
			db.Memory.Set(key, value)
		}
		for key, value := range database.expirableData {
			db.Memory.Set(key, value)
		}
	}
}

func Init(ctx *context.Context) *PersiDb {
	db := PersiDb{
		ctx:        ctx,
		logger:     ctx.Logger,
		dbDir:      ctx.ServerInstance.GetRdbDir(),
		dbFileName: ctx.ServerInstance.GetRdbFileName(),
		Memory:     initMemory(),
		streamMap:  make(map[string]*Stream),
	}
	go db.load()
	go db.garbageCollector()
	return &db
}

func (db *PersiDb) Persist(key string, value []byte, options SetOptions) error {
	valueToPersist := Value{
		Data: value,
		Type: options.ValueType,
	}

	if options.ExpiryDuration > 0 {
		expirationTime := time.Now().Add(options.ExpiryDuration)
		valueToPersist.ExpirationTime = &expirationTime
	}
	db.logger.Printf("For key: %s persisting value: %s", key, value)
	err := db.Memory.Set(key, valueToPersist)
	return err
}

func (db *PersiDb) Fetch(key string) (*Value, bool) {
	db.logger.Printf("Fetching value for key: %s", key)
	value, valueExists := db.Memory.Get(key)
	if !valueExists {
		db.logger.Printf("No value found against key: %s", key)
		return nil, false
	}

	isKeyExpired := (value.ExpirationTime != nil && time.Now().After(*value.ExpirationTime))
	if !isKeyExpired {
		// If value exists and it hasn't expired, return the value
		db.logger.Printf("Value fetched for key '%s' is: %q", key, value.Data)
		return &value, true
	}
	db.logger.Printf("Value '%q' against  key '%s' has expired", value.Data, key)
	db.Memory.DeleteExpired(key)
	return nil, false
}

func (db *PersiDb) createStream(streamKey string) (*Stream, error) {
	stream, streamExists := db.streamMap[streamKey]
	if streamExists {
		return stream, nil
	}
	stream = NewStream(streamKey)
	db.streamMap[streamKey] = stream
	return stream, nil

}

func (db *PersiDb) getStream(streamKey string) (*Stream, bool) {
	stream, streamExists := db.streamMap[streamKey]
	return stream, streamExists
}

func (db *PersiDb) AddToStream(streamKey string, persistId string, fieldValuePairs [][]byte) (string, error) {
	var err error
	stream, streamExists := db.streamMap[streamKey]
	if !streamExists {
		stream, err = db.createStream(streamKey)
		if err != nil {
			return "", err
		}
	}
	persistedId, err := stream.Add(persistId, fieldValuePairs)
	if err != nil {
		return "", err
	}
	return persistedId, nil
}

func (db *PersiDb) GetKeysWithPattern(pattern string) []string {
	keys := db.Memory.GetAllKeys()
	matchedKeys := make([]string, 0)
	for _, key := range keys {
		if match, _ := match(pattern, key); match {
			matchedKeys = append(matchedKeys, key)
		}
	}
	for streamKey := range db.streamMap {
		if match, _ := match(pattern, streamKey); match {
			matchedKeys = append(matchedKeys, streamKey)
		}
	}
	return matchedKeys
}

func (db *PersiDb) GetKeyType(key string) string {
	value, valueExists := db.Memory.Get(key)
	if !valueExists {
		_, streamExists := db.getStream(key)
		if !streamExists {
			return constants.NONE
		}
		return constants.STREAM
	}
	return value.Type
}

func (db *PersiDb) garbageCollector() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		for key, val := range db.Memory.GetAllExpirableKeyValuePair() {
			if val.ExpirationTime != nil && now.After(*val.ExpirationTime) {
				db.Memory.DeleteExpired(key)
				db.logger.Printf("Deleted expired key: %s", key)
			}
		}
	}
}

func match(pattern string, key string) (bool, error) {
	if pattern == "*" {
		return true, nil
	}
	// TODO: Implement pattern matching
	return false, nil
}
