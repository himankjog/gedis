package persistence

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix"
)

type RecordIdType byte

const (
	DEFAULT_ID_FORMAT = "%d-%d"

	NEW_ID        RecordIdType = 0x0
	INCOMPLETE_ID RecordIdType = 0x1
	COMPLETE_ID   RecordIdType = 0x2
	CUSTOM_ID     RecordIdType = 0x3
)

type RecordId struct {
	Epoch int64
	Count int64
}

type Stream struct {
	StreamName     string
	storage        *iradix.Tree
	lastRecordedId *RecordId
	uidLock        *sync.RWMutex
}

func NewStream(streamName string) *Stream {
	stream := Stream{
		StreamName: streamName,
		storage:    iradix.New(),
		lastRecordedId: &RecordId{
			Epoch: 0,
			Count: 0,
		},
		uidLock: &sync.RWMutex{},
	}
	fmt.Printf("Created new stream: %s\n", streamName)
	return &stream
}

func getStreamEntryIdType(id string) RecordIdType {
	if id == "*" {
		return NEW_ID
	}
	isIncompleteId, _ := regexp.MatchString(`^(\d+)-\*$`, id)
	if isIncompleteId {
		return INCOMPLETE_ID
	}
	isCompleteId, _ := regexp.MatchString(`^\d+-\d+$`, id)
	if isCompleteId {
		return COMPLETE_ID
	}
	return CUSTOM_ID
}

func (s *Stream) getId(providedId string) (string, error) {
	s.uidLock.Lock()
	defer s.uidLock.Unlock()

	switch getStreamEntryIdType(providedId) {
	case NEW_ID:
		currentTimeInMs := time.Now().UnixMilli()
		lastRecordId := s.lastRecordedId
		newRecordIdTimeEpochMs := currentTimeInMs
		newRecordIdCount := int64(0)

		if lastRecordId.Epoch == newRecordIdTimeEpochMs {
			newRecordIdCount = lastRecordId.Count + 1
		}
		lastRecordId.Count = newRecordIdCount
		lastRecordId.Epoch = newRecordIdTimeEpochMs
		return fmt.Sprintf(DEFAULT_ID_FORMAT, newRecordIdTimeEpochMs, newRecordIdCount), nil
	case INCOMPLETE_ID:
		idTimeEpochMs, _ := strconv.ParseInt(providedId[:len(providedId)-2], 10, 64)
		lastRecordId := s.lastRecordedId
		newRecordIdTimeEpochMs := idTimeEpochMs
		newRecordIdCount := int64(0)

		if lastRecordId.Epoch == newRecordIdTimeEpochMs {
			newRecordIdCount = lastRecordId.Count + 1
		} else if lastRecordId.Epoch < newRecordIdTimeEpochMs {
			newRecordIdCount = 0
		} else {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		lastRecordId.Count = newRecordIdCount
		lastRecordId.Epoch = newRecordIdTimeEpochMs
		return fmt.Sprintf(DEFAULT_ID_FORMAT, idTimeEpochMs, newRecordIdCount), nil
	case COMPLETE_ID:
		parts := strings.Split(providedId, "-")
		epochTimeInMs, _ := strconv.ParseInt(parts[0], 10, 64)
		itemCount, _ := strconv.ParseInt(parts[1], 10, 64)
		lastRecordId := s.lastRecordedId

		if epochTimeInMs == 0 && itemCount == 0 {
			// Special case where no entry could be added with 0-0 id
			return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}
		if epochTimeInMs >= lastRecordId.Epoch && itemCount > lastRecordId.Count {
			lastRecordId.Count = itemCount
			lastRecordId.Epoch = epochTimeInMs
			return providedId, nil
		}
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	default:
		return providedId, nil
	}
}

func (s *Stream) Add(id string, fieldValuePairs [][]byte) (string, error) {
	persistedId, err := s.getId(id)
	if err != nil {
		return "", err
	}
	treeRef, _, _ := s.storage.Insert([]byte(persistedId), fieldValuePairs)
	s.storage = treeRef
	return persistedId, nil
}
