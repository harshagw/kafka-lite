package main

import (
	"fmt"
	"os"
)

type RecordValueHeader struct {
	FrameVersion int8
	RecordType   int8
	Version      int8
}

type  FeatureLevelRecordValue struct {
	Header RecordValueHeader
	Name string
	FeatureLevel int16
}

type TopicRecordValue struct {
	Header RecordValueHeader
	Name   string
	Id     UUID
}

type PartitionRecordValue struct {
	Header RecordValueHeader
	PartitionId int32
	TopicId     UUID
	Replicas    []int32
	InSyncReplicas         []int32
	RemovingReplicas       []int32
	AddingReplicas         []int32
	Leader int32
	LeaderEpoch int32
	PartitionEpoch int32
	Directories []byte // implement this later properly
}

type Record struct {
	Length         int64
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []byte // To be fixed later
}

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	MagicByte            int8
	Crc                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	FirstSequence        int32
	Records              []Record
}

var topicNameToTopicId = make(map[string]UUID)
var topicIdToPartitions = make(map[UUID][]PartitionRecordValue)
var partitionIdToPartition = make(map[int32]PartitionRecordValue)
var featureLevelRecordValues = make([]FeatureLevelRecordValue, 0)
var topicRecordValues = make([]TopicRecordValue, 0)

func readLogFile(filePath string) ([]*RecordBatch, error) {
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %w", err)
	}

	reader := NewKafkaReader(fileBytes)

	batches := make([]*RecordBatch, 0)
	for reader.Remaining() > 0 {
		batch := RecordBatch{}

		baseOffset, err := reader.Int64()
		if err != nil {
			return nil, fmt.Errorf("unable to read base offset: %w", err)
		}
		batch.BaseOffset = int64(baseOffset)

		batchLength, err := reader.Int32()
		if err != nil {
			return nil, fmt.Errorf("unable to read batch length: %w", err)
		}
		batch.BatchLength = batchLength

		// Check if we have enough bytes for the batch
		// If batch length is larger than remaining data, use all remaining data
		actualBatchLength := int(batchLength)
		if reader.Remaining() < actualBatchLength {
			actualBatchLength = reader.Remaining()
		}

		// Read the batch data
		batchData, err := reader.Bytes(actualBatchLength)
		if err != nil {
			return nil, fmt.Errorf("unable to read batch data: %w", err)
		}

		// Create a new reader for the batch data
		batchReader := NewKafkaReader(batchData)

		partitionLeaderEpoch, err := batchReader.Int32()
		if err != nil {
			return nil, fmt.Errorf("unable to read partition leader epoch: %w", err)
		}
		batch.PartitionLeaderEpoch = partitionLeaderEpoch

		magicByte, err := batchReader.Int8()
		if err != nil {
			return nil, fmt.Errorf("unable to read magic byte: %w", err)
		}
		batch.MagicByte = magicByte

		crc, err := batchReader.Int32()
		if err != nil {
			return nil, fmt.Errorf("unable to read CRC: %w", err)
		}
		batch.Crc = int32(crc)

		attributes, err := batchReader.Int16()
		if err != nil {
			return nil, fmt.Errorf("unable to read attributes: %w", err)
		}
		batch.Attributes = attributes
		
		lastOffsetDelta, err := batchReader.Int32()
		if err != nil {
			return nil, fmt.Errorf("unable to read last offset delta: %w", err)
		}
		batch.LastOffsetDelta = lastOffsetDelta

		baseTimestamp, err := batchReader.Int64()
		if err != nil {
			return nil, fmt.Errorf("unable to read base timestamp: %w", err)
		}
		batch.BaseTimestamp = baseTimestamp

		maxTimestamp, err := batchReader.Int64()
		if err != nil {
			return nil, fmt.Errorf("unable to read max timestamp: %w", err)
		}
		batch.MaxTimestamp = maxTimestamp

		producerId, err := batchReader.Int64()
		if err != nil {
			return nil, fmt.Errorf("unable to read producer ID: %w", err)
		}
		batch.ProducerId = int64(producerId)

		producerEpoch, err := batchReader.Int16()
		if err != nil {
			return nil, fmt.Errorf("unable to read producer epoch: %w", err)
		}
		batch.ProducerEpoch = producerEpoch

		firstSequence, err := batchReader.Int32()
		if err != nil {
			return nil, fmt.Errorf("unable to read first sequence: %w", err)
		}
		batch.FirstSequence = firstSequence

		noOfRecords, err := batchReader.Int32()
		if err != nil {
			return nil, fmt.Errorf("unable to read number of records: %w", err)
		}

		records := make([]Record, noOfRecords)
		for i := 0; i < int(noOfRecords); i++ {			
			recordLength, err := batchReader.VarInt()
			if err != nil {
				return nil, fmt.Errorf("unable to read record length: %w", err)
			}

			recordBytes, err := batchReader.Bytes(int(recordLength))
			if err != nil {
				return nil, fmt.Errorf("unable to read record bytes: %w", err)
			}

			recordReader := NewKafkaReader(recordBytes)

			attributes, err := recordReader.Int8()
			if err != nil {
				return nil, fmt.Errorf("unable to read record attributes: %w", err)
			}

			timestampDelta, err := recordReader.VarInt()
			if err != nil {
				return nil, fmt.Errorf("unable to read record timestamp delta: %w", err)
			}
			
			offsetDelta, err := recordReader.VarInt()
			if err != nil {
				return nil, fmt.Errorf("unable to read record offset delta: %w", err)
			}

			keyLength, err := recordReader.VarInt()
			if err != nil {
				return nil, fmt.Errorf("unable to read record key length: %w", err)
			}
			
			key := make([]byte, 0)

			if keyLength > 0 {
				key = make([]byte, keyLength)
				key, err = recordReader.Bytes(int(keyLength))
				if err != nil {
					return nil, fmt.Errorf("unable to read record key: %w", err)
				}
			}

			valueLength, err := recordReader.VarInt()
			if err != nil {
				return nil, fmt.Errorf("unable to read record value length: %w", err)
			}

			value := make([]byte, valueLength)
			if valueLength > 0 {
				value, err = recordReader.Bytes(int(valueLength))
				if err != nil {
					return nil, fmt.Errorf("unable to read record value: %w", err)
				}
			}

			record := Record{
				Length: recordLength,
				Attributes: attributes,
				TimestampDelta: timestampDelta,
				OffsetDelta: offsetDelta,
				Key: key,
				Value: value,
			}
			records[i] = record
		}
		batch.Records = records

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
			
			valueReader := NewKafkaReader(value)
			// we can get the frame version, type and version from each
			frameVersion, err := valueReader.Int8()
			if err != nil {
				return err
			}
			recordType, err := valueReader.Int8()
			if err != nil {
				return err
			}
			version, err := valueReader.Int8()
			if err != nil {
				return err
			}
			header := RecordValueHeader{
				FrameVersion: frameVersion,
				RecordType: recordType,
				Version: version,
			}
			if recordType == 12 {
				// Feature level record
				nameLength, err := valueReader.VarUint()
				if err != nil {
					return err
				}
				nameLength--
				name, err := valueReader.Bytes(int(nameLength))
				if err != nil {
					return err
				}
				featureLevel, err := valueReader.Int16()
				if err != nil {
					return err
				}
				featureLevelRecordValue := FeatureLevelRecordValue{
					Header: header,
					Name: string(name),
					FeatureLevel: featureLevel,
				}
				featureLevelRecordValues = append(featureLevelRecordValues, featureLevelRecordValue)
			} else if recordType == 2 {
				// Topic record
				nameLength, err := valueReader.VarUint()
				if err != nil {
					return err
				}
				nameLength--
				name, err := valueReader.Bytes(int(nameLength))
				if err != nil {
					return err
				}
				topicUUID, err := valueReader.Bytes(16)
				if err != nil {
					return err
				}
				topicNameToTopicId[string(name)] = UUID(topicUUID[:])
				
				topicRecordValue := TopicRecordValue{
					Header: header,
					Name: string(name),
					Id: UUID(topicUUID[:]),
				}
				topicRecordValues = append(topicRecordValues, topicRecordValue)
			} else if recordType == 3 {
				// Partition record
				partitionId, err := valueReader.Int32()
				if err != nil {
					return err
				}
				topicId, err := valueReader.Bytes(16)
				if err != nil {
					return err
				}
				replicas, err := valueReader.CompactInt32Array()
				if err != nil {
					return err
				}
				inSyncReplicas, err := valueReader.CompactInt32Array()
				if err != nil {
					return err
				}
				removingReplicas, err := valueReader.CompactInt32Array()
				if err != nil {
					return err
				}
				addingReplicas, err := valueReader.CompactInt32Array()
				if err != nil {
					return err
				}
				leader, err := valueReader.Int32()
				if err != nil {
					return err
				}
				leaderEpoch, err := valueReader.Int32()
				if err != nil {
					return err
				}
				partitionEpoch, err := valueReader.Int32()
				if err != nil {
					return err
				}
				directoriesLength, err := valueReader.VarUint()
				if err != nil {
					return err
				}
				directories := []byte{}
				if directoriesLength <= 1 {
					directories = []byte{}
				} else {
					directoriesLength--
					directories, err = valueReader.Bytes(int(directoriesLength))
					if err != nil {
						return err
					}
				}
				topicUUID := UUID(topicId[:])
				partitionRecordValue := PartitionRecordValue{
					Header: header,
					PartitionId: partitionId,
					TopicId: topicUUID,
					Replicas: replicas,
					InSyncReplicas: inSyncReplicas,
					RemovingReplicas: removingReplicas,
					AddingReplicas: addingReplicas,
					Leader: leader,
					LeaderEpoch: leaderEpoch,
					PartitionEpoch: partitionEpoch,
					Directories: directories,
				}
				partitionIdToPartition[partitionId] = partitionRecordValue
				topicIdToPartitions[topicUUID] = append(topicIdToPartitions[topicUUID], partitionRecordValue)
			}
		}
	}

	return nil
}