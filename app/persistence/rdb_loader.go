package persistence

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

// OpCodes
const (
	AUX           = 0xFA
	RESIZEDB      = 0xFB
	EXPIRETIME_MS = 0xFC
	EXPIRETIME    = 0xFD
	SELECTDB      = 0xFE
	EOF           = 0xFF
)

// Value Types
const (
	STRING ValueType = 0x00
)

type LengthEncodingType byte

// Length encoding types
const (
	LENGTH_6BIT    LengthEncodingType = 0x00
	LENGTH_14BIT   LengthEncodingType = 0x01
	LENGTH_32BIT   LengthEncodingType = 0x02
	LENGTH_SPECIAL LengthEncodingType = 0x03

	SIX_BIT_MASK = 0x3F
)

// Size encoding types
const (
	INT_8BIT  = 0
	INT_16BIT = 1
	INT_32BIT = 2
)

type LengthEncodingResponse struct {
	ReturnValue        uint64
	LengthEncodingType LengthEncodingType
}

type IndexedDb struct {
	expirableData    map[string]Value
	nonExpirableData map[string]Value
	index            int
}

type LoadRDBResponse struct {
	arbitraryMetaData map[string]string
	dbs               map[int]*IndexedDb
}

func (rdb *LoadRDBResponse) AddDb(databaseIndex int, db *IndexedDb) {
	db.index = databaseIndex
	rdb.dbs[databaseIndex] = db
}

func readNextNBytes(reader *bytes.Reader, n int) ([]byte, error) {
	nextBytes := make([]byte, n)
	_, err := reader.Read(nextBytes)
	if err != nil {
		fmt.Printf("Error while trying to read next %d bytes: %v", n, err.Error())
		return nil, err
	}
	return nextBytes, nil
}

func readNextByte(reader *bytes.Reader) (byte, error) {
	nextBytes, err := readNextNBytes(reader, 1)
	if err != nil {
		return 0, err
	}
	return nextBytes[0], nil
}

func readLengthEncoding(reader *bytes.Reader) (*LengthEncodingResponse, error) {
	lengthEncodingByte, err := readNextByte(reader)
	if err != nil {
		return nil, err
	}
	lengthEncodingType := LengthEncodingType(lengthEncodingByte >> 6)
	lengthEncodingResponse := LengthEncodingResponse{
		ReturnValue:        0,
		LengthEncodingType: lengthEncodingType,
	}
	fmt.Printf("Length encoding byte: %d", lengthEncodingByte)
	switch lengthEncodingType {
	case LENGTH_6BIT:
		length := uint64(lengthEncodingByte & SIX_BIT_MASK)
		lengthEncodingResponse.ReturnValue = length
		fmt.Printf("Read 6 bit length encoding. Length: %d", length)
		return &lengthEncodingResponse, nil
	case LENGTH_14BIT:
		nextLengthEncodingByte, err := readNextByte(reader)
		if err != nil {
			return nil, err
		}
		length := (uint64(lengthEncodingByte&SIX_BIT_MASK) << 8) | uint64(nextLengthEncodingByte)
		lengthEncodingResponse.ReturnValue = length
		fmt.Printf("Read 14 bit length encoding. Length: %d", length)
		return &lengthEncodingResponse, nil
	case LENGTH_32BIT:
		lengthBytes, err := readNextNBytes(reader, 4)
		if err != nil {
			return nil, err
		}
		length := (uint64(lengthBytes[0]) << 24) | (uint64(lengthBytes[1]) << 16) | (uint64(lengthBytes[2]) << 8) | uint64(lengthBytes[3])
		lengthEncodingResponse.ReturnValue = length
		fmt.Printf("Read 32 bit length encoding. Length: %d", length)
		return &lengthEncodingResponse, nil
	case LENGTH_SPECIAL:
		length := uint64(lengthEncodingByte & SIX_BIT_MASK)
		lengthEncodingResponse.ReturnValue = length
		fmt.Printf("Read 6 bit special string encoding type: %d", length)
		return &lengthEncodingResponse, nil
	default:
		return nil, fmt.Errorf("invalid length encoding type: %d", lengthEncodingType)
	}
}

func readStringEncoding(reader *bytes.Reader) (string, error) {
	lengthEncodingResponse, err := readLengthEncoding(reader)
	if err != nil {
		return "", err
	}
	lengthEncodingResponseValue := lengthEncodingResponse.ReturnValue
	if lengthEncodingResponse.LengthEncodingType == LENGTH_SPECIAL {
		switch lengthEncodingResponseValue {
		case INT_8BIT:
			intByte, err := readNextByte(reader)
			if err != nil {
				fmt.Printf("error while trying to read 8 bit int encoded as string: %v", err.Error())
				return "", err
			}
			return strconv.FormatInt(int64(intByte), 10), nil
		case INT_16BIT:
			lengthBytes, err := readNextNBytes(reader, 2)
			if err != nil {
				fmt.Printf("Error while trying to read 16 bit int encoded as string: %v", err.Error())
				return "", err
			}
			val := (int16(lengthBytes[1]) << 8) | int16(lengthBytes[0])
			return strconv.FormatInt(int64(val), 10), nil
		case INT_32BIT:
			lengthBytes, err := readNextNBytes(reader, 4)
			if err != nil {
				fmt.Printf("Error while trying to read 32 bit int encoded as string: %v", err.Error())
				return "", err
			}
			val := (int32(lengthBytes[3]) << 24) | (int32(lengthBytes[2]) << 16) | (int32(lengthBytes[1]) << 8) | int32(lengthBytes[0])
			return strconv.FormatInt(int64(val), 10), nil
		default:
			return "", fmt.Errorf("unsupported string encoding type")
		}
	}
	stringBytes, err := readNextNBytes(reader, int(lengthEncodingResponseValue))
	if err != nil {
		fmt.Printf("Error while trying to read encoded string: %v", err.Error())
		return "", err
	}
	return string(stringBytes), nil
}

func readStringEncodingKeyValuePair(reader *bytes.Reader) ([]byte, error) {
	value, err := readStringEncoding(reader)
	if err != nil {
		fmt.Printf("Error while trying to read string-encoded value: %v", err.Error())
		return nil, err
	}
	return []byte(value), nil
}

func readKeyValuePair(reader *bytes.Reader, valueType ValueType) (string, []byte, error) {
	key, err := readStringEncoding(reader)
	if err != nil {
		fmt.Printf("Error while trying to read string-encoded key: %v", err.Error())
		return "", nil, err
	}
	switch valueType {
	case STRING:
		value, err := readStringEncodingKeyValuePair(reader)
		return key, value, err
	default:
		return "", nil, fmt.Errorf("unsupported value type: %d", valueType)
	}
}

func readTableSize(reader *bytes.Reader) (int, error) {
	tableSize, err := readLengthEncoding(reader)
	if err != nil {
		return 0, err
	}
	return int(tableSize.ReturnValue), nil
}

func validateChecksum(reader *bytes.Reader) bool {
	checksumBytes, err := readNextNBytes(reader, 8)
	if err != nil {
		fmt.Printf("Error while trying to read checksum: %v", err.Error())
		return false
	}
	fmt.Printf("Checksum: %v", checksumBytes)
	return true
}

func readDatabaseIndex(reader *bytes.Reader) (int, error) {
	databaseIndex, err := readLengthEncoding(reader)
	if err != nil {
		return 0, err
	}
	return int(databaseIndex.ReturnValue), nil
}

func validateMetaData(reader *bytes.Reader) (string, string, bool) {
	metaDataKey, metaDataValueBytes, err := readKeyValuePair(reader, STRING)
	if err != nil {
		fmt.Printf("Error while trying to read metadata key-value pair: %v", err.Error())
		return "", "", false
	}
	metaDataValue := string(metaDataValueBytes)
	fmt.Printf("Metadata key: %s, Metadata value: %s", metaDataKey, metaDataValue)
	// TODO: Validate metadata
	return metaDataKey, metaDataValue, true
}

func validateHeaderSection(reader *bytes.Reader) bool {
	magicStringBytes, err := readNextNBytes(reader, 5)
	if err != nil {
		fmt.Printf("Error while trying to read magic string: %v", err.Error())
		return false
	}
	magicString := string(magicStringBytes)
	if magicString != "REDIS" {
		fmt.Printf("Invalid magic string: %s", magicString)
		return false
	}
	fmt.Printf("Magic string: %s", magicString)
	rdbVersionNumber, err := readNextNBytes(reader, 4)
	if err != nil {
		fmt.Printf("Error while trying to read RDB version number: %v", err.Error())
		return false
	}
	rdbVersion := string(rdbVersionNumber)
	fmt.Printf("RDB version: %s", rdbVersion)
	return true
}

func loadDatabase(reader *bytes.Reader) (*IndexedDb, error) {
	indexedDb := IndexedDb{
		expirableData:    make(map[string]Value, 0),
		nonExpirableData: make(map[string]Value, 0),
	}
	continueLoading := true
	for continueLoading {
		opcode, err := readNextByte(reader)
		if err != nil {
			return nil, err
		}
		switch opcode {
		case RESIZEDB:
			// Database size information
			hashTableSize, err := readTableSize(reader)
			if err != nil {
				return nil, err
			}
			fmt.Printf("Hash table size: %d", hashTableSize)
			indexedDb.expirableData = make(map[string]Value, hashTableSize)
			expireHashTableSize, err := readTableSize(reader)
			if err != nil {
				return nil, err
			}
			fmt.Printf("Expire hash table size: %d", expireHashTableSize)
			indexedDb.nonExpirableData = make(map[string]Value, expireHashTableSize)
		case EXPIRETIME_MS:
			expiryTimeInMs, err := readLengthEncoding(reader)
			if err != nil {
				return nil, err
			}
			fmt.Printf("Expiry time in milliseconds: %d", expiryTimeInMs)
			nextByte, err := readNextByte(reader)
			if err != nil {
				return nil, err
			}

			valueType := ValueType(nextByte)
			fmt.Printf("[EXPIRABLE] Value type: %d", valueType)
			key, val, err := readKeyValuePair(reader, valueType)
			if err != nil {
				return nil, err
			}
			fmt.Printf("[EXPIRABLE] Key: %s, Value: %q", key, val)
			// expiration time is the epoch timestamp in milliseconds
			// Convert epoch timestamp to time.Time object
			expiryTime := time.Unix(0, int64(expiryTimeInMs.ReturnValue)*int64(time.Millisecond))
			if time.Now().After(expiryTime) {
				fmt.Printf("Key: %s has expired, not adding to database", key)
				continue
			}
			indexedDb.expirableData[key] = Value{
				Data:           val,
				ValueType:      valueType,
				ExpirationTime: &expiryTime,
			}

		case EXPIRETIME:
			expiryTimeInSeconds, err := readLengthEncoding(reader)
			if err != nil {
				return nil, err
			}
			fmt.Printf("Expiry time in seconds: %d", expiryTimeInSeconds)
			nextByte, err := readNextByte(reader)
			if err != nil {
				return nil, err
			}
			valueType := ValueType(nextByte)
			fmt.Printf("[EXPIRABLE] Value type: %d", valueType)
			key, val, err := readKeyValuePair(reader, valueType)
			if err != nil {
				return nil, err
			}
			fmt.Printf("[EXPIRABLE] Key: %s, Value: %q", key, val)
			// expiration time is the epoch timestamp in milliseconds
			// Convert epoch timestamp to time.Time object
			expiryTime := time.Unix(int64(expiryTimeInSeconds.ReturnValue), 0)
			if time.Now().After(expiryTime) {
				fmt.Printf("Key: %s has expired, not adding to database", key)
				continue
			}
			indexedDb.expirableData[key] = Value{
				Data:           val,
				ValueType:      valueType,
				ExpirationTime: &expiryTime,
			}
		case EOF, SELECTDB:
			continueLoading = false
		default:
			// Read key value pair
			valueType := ValueType(opcode)
			key, val, err := readKeyValuePair(reader, valueType)
			if err != nil {
				return nil, err
			}
			fmt.Printf("Key: %s, Value: %q", key, val)
			indexedDb.nonExpirableData[key] = Value{
				Data:           val,
				ValueType:      valueType,
				ExpirationTime: nil,
			}
		}
	}
	return &indexedDb, nil
}

func parseRdb(data []byte) (*LoadRDBResponse, error) {
	// Parse data and load into memory
	reader := bytes.NewReader(data)
	hasValidHeader := validateHeaderSection(reader)

	if !hasValidHeader {
		return nil, fmt.Errorf("invalid RDB file header")
	}

	loadedRDB := LoadRDBResponse{
		arbitraryMetaData: make(map[string]string),
		dbs:               make(map[int]*IndexedDb),
	}
	continueParsing := true
	for continueParsing {
		opcode, err := readNextByte(reader)
		if err != nil {
			return nil, err
		}
		switch opcode {
		case AUX:
			// Meta data section
			metaDataKey, metaDataVal, hasValidMetaData := validateMetaData(reader)
			if !hasValidMetaData {
				return nil, fmt.Errorf("invalid metadata")
			}
			loadedRDB.arbitraryMetaData[metaDataKey] = metaDataVal
		case SELECTDB:
			// Database index section
			databaseIndex, err := readDatabaseIndex(reader)
			if err != nil {
				return nil, err
			}
			fmt.Printf("Reading database index: %d", databaseIndex)
			loadedDatabase, err := loadDatabase(reader)
			if err != nil {
				return nil, err
			}
			loadedRDB.AddDb(databaseIndex, loadedDatabase)
			// Move reader 2 places back so that the next opcode can be read
			reader.Seek(-2, 1)
		case EOF:
			validChecksum := validateChecksum(reader)
			if !validChecksum {
				return nil, fmt.Errorf("invalid checksum")
			}
			continueParsing = false
		}
	}
	return &loadedRDB, nil
}

func LoadRDB(filePath string) (*LoadRDBResponse, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error while trying to open file at path '%s': %v", filePath, err.Error())
		return nil, err
	}
	defer file.Close()

	// Read all file content at once
	data, err := os.ReadFile(filePath)

	if err != nil {
		fmt.Printf("Error while reading file at path '%s': %v", filePath, err.Error())
		return nil, err
	}
	fmt.Printf("Successfully read %d bytes from file '%s'", len(data), filePath)

	return parseRdb(data)
}
