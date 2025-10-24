package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/kafka-starter-go/app/ktypes"
)

type RecordValueHeader struct {
	FrameVersion ktypes.Int8 `order:"1"`
	RecordType   ktypes.Int8 `order:"2"`
	Version      ktypes.Int8 `order:"3"`
}

type  FeatureLevelRecordValue struct {
	Header RecordValueHeader `order:"1"`
	Name ktypes.String `order:"2"`
	FeatureLevel ktypes.Int16 `order:"3"`
}

type TopicRecordValue struct {
	Header RecordValueHeader `order:"1"`
	Name   ktypes.String `order:"2"`
	Id     ktypes.UUID `order:"3"`
}

type PartitionRecordValue struct {
	Header RecordValueHeader `order:"1"`
	PartitionId ktypes.Int32 `order:"2"`
	TopicId     ktypes.UUID `order:"3"`
	Replicas    ktypes.CompactArray[ktypes.Int32] `order:"4"`
	InSyncReplicas         ktypes.CompactArray[ktypes.Int32] `order:"5"`
	RemovingReplicas       ktypes.CompactArray[ktypes.Int32] `order:"6"`
	AddingReplicas         ktypes.CompactArray[ktypes.Int32] `order:"7"`
	Leader ktypes.Int32 `order:"8"`
	LeaderEpoch ktypes.Int32 `order:"9"`
	PartitionEpoch ktypes.Int32 `order:"10"`
	Directories ktypes.CompactBytes `order:"11"` // implement this later properly
}

type Record struct {
	Length         ktypes.VarInt `order:"1"`
	Attributes     ktypes.Int8 `order:"2"`
	TimestampDelta ktypes.VarInt `order:"3"`
	OffsetDelta    ktypes.VarInt `order:"4"`
	Key            ktypes.Bytes `order:"5"`
	Value          ktypes.Bytes `order:"6"`
	Headers        ktypes.Bytes `order:"7"` // To be fixed later
}

type RecordBatch struct {
	BaseOffset           ktypes.Int64 `order:"1"`
	BatchLength          ktypes.Int32 `order:"2"`
	PartitionLeaderEpoch ktypes.Int32 `order:"3"`
	MagicByte            ktypes.Int8 `order:"4"`
	Crc                  ktypes.Int32 `order:"5"`
	Attributes           ktypes.Int16 `order:"6"`
	LastOffsetDelta      ktypes.Int32 `order:"7"`
	BaseTimestamp        ktypes.Int64 `order:"8"`
	MaxTimestamp         ktypes.Int64 `order:"9"`
	ProducerId           ktypes.Int64 `order:"10"`
	ProducerEpoch        ktypes.Int16 `order:"11"`
	FirstSequence        ktypes.Int32 `order:"12"`
	Records              ktypes.Array[Record] `order:"13"`
}

var topicNameToTopicId = make(map[string]ktypes.UUID)

var topicIdToTopicName = make(map[ktypes.UUID]string)
var topicIdToTopicRecord = make(map[ktypes.UUID][]TopicRecordValue)
var topicIdToPartitions = make(map[ktypes.UUID][]PartitionRecordValue)
var topicIdToPartitionIds = make(map[ktypes.UUID][]int32)

var featureLevelRecordValues = make([]FeatureLevelRecordValue, 0)


// Returns the log file as an array of RecordBatch.
func readLogFile(filePath string) ([]*RecordBatch, error) {
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %w", err)
	}
	decoder := ktypes.NewKDecoder(fileBytes)

	batches := make([]*RecordBatch, 0)
	for decoder.HasMoreData() {
		// Check if we have enough bytes for a minimal RecordBatch
		// Minimum is: BaseOffset (8) + BatchLength (4) = 12 bytes
		if decoder.RemainingBytes() < 12 {
			// Not enough bytes for another RecordBatch, break
			break
		}
		
		var batch RecordBatch
		if err := decoder.Decode(&batch); err != nil {
			return nil, fmt.Errorf("unable to decode the record batch: %w", err)
		}
		batches = append(batches, &batch)
	}

	return batches, nil
}

func prepareLogFileData(fileName string) (error) {
	batches, err := readLogFile(fileName)
	if err != nil {
		return err
	}

	for _, batch := range batches {
		for _, record := range batch.Records {
			value := record.Value
			
			// Skip records with no value data
			if len(value) == 0 {
				continue
			}
			
			valueDecoder := ktypes.NewKDecoder(value)
			var header RecordValueHeader
			if err := valueDecoder.Decode(&header); err != nil {
				return err
			}
			if header.RecordType == ktypes.Int8(12) {
				// Feature level record
				var featureLevelRecord FeatureLevelRecordValue
				featureLevelRecord.Header = header
				if err := valueDecoder.Decode(&featureLevelRecord); err != nil {
					return err
				}
				featureLevelRecordValues = append(featureLevelRecordValues, featureLevelRecord)
			} else if header.RecordType == ktypes.Int8(2) {
				// Topic record
				var topicRecord TopicRecordValue
				topicRecord.Header = header
				if err := valueDecoder.Decode(&topicRecord); err != nil {
					return err
				}
				topicNameToTopicId[string(topicRecord.Name)] = topicRecord.Id
				topicIdToTopicName[topicRecord.Id] = string(topicRecord.Name)
				topicIdToTopicRecord[topicRecord.Id] = append(topicIdToTopicRecord[topicRecord.Id], topicRecord)
			} else if header.RecordType == ktypes.Int8(3) {
				// Partition record
				var partitionRecord PartitionRecordValue
				partitionRecord.Header = header
				if err := valueDecoder.Decode(&partitionRecord); err != nil {
					return err
				}
				topicIdToPartitions[partitionRecord.TopicId] = append(topicIdToPartitions[partitionRecord.TopicId], partitionRecord)
				topicIdToPartitionIds[partitionRecord.TopicId] = append(topicIdToPartitionIds[partitionRecord.TopicId], int32(partitionRecord.PartitionId))
			}
		}
	}

	return nil
}


func getTopicPartitionRecords(topicId ktypes.UUID, partitionIndex int32) ([]byte, error) {
	topicName := topicIdToTopicName[topicId]
	folderPath := LOGS_BASE_FOLDER + topicName + "-" + strconv.Itoa(int(partitionIndex))
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		return nil, err
	}

	files, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return []byte{}, nil
	}

	records := make([]byte, 0)
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".log") {
			continue
		}

		filePath := folderPath + "/" + file.Name()
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		records = append(records, fileBytes...)
	}

	return records, nil
}