package persistence

import (
	"fmt"
	"sync"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix"
)

const (
	DEFAULT_ID_FORMAT = "%d-%d"
)

type Stream struct {
	StreamName         string
	storage            *iradix.Tree
	currentMsItemCount int64
	uidLock            *sync.RWMutex
}

func NewStream(streamName string) *Stream {
	stream := Stream{
		StreamName:         streamName,
		storage:            iradix.New(),
		currentMsItemCount: 0,
		uidLock:            &sync.RWMutex{},
	}
	fmt.Printf("Created new stream: %s\n", streamName)
	return &stream
}

func (s *Stream) getId(providedId string, recordAdditionTimeEpochMs int64) string {
	if providedId != "*" {
		// TODO: Handle case of incomplete ID that consists of epoch time part
		return providedId
	}
	s.uidLock.Lock()
	defer s.uidLock.Unlock()
	currentTimeEpochMs := time.Now().UnixMilli()
	currentMsItemCount := s.currentMsItemCount
	if currentTimeEpochMs == recordAdditionTimeEpochMs {
		s.currentMsItemCount++
	} else {
		s.currentMsItemCount = 0
		currentMsItemCount = 0
	}
	return fmt.Sprintf(DEFAULT_ID_FORMAT, currentTimeEpochMs, currentMsItemCount)
}

func (s *Stream) Add(id string, fieldValuePairs [][]byte) (string, error) {
	currentTimeInMs := time.Now().UnixMilli()
	persistedId := s.getId(id, currentTimeInMs)
	treeRef, _, _ := s.storage.Insert([]byte(persistedId), fieldValuePairs)
	s.storage = treeRef
	return persistedId, nil
}
