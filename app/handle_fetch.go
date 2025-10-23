package main

import "slices"

type FetchResponsePartitionAbortedTransaction struct {
	ProducerId int64
	FirstOffset int64
}

type FetchResponsePartition struct {
	PartitionIndex int32
	ErrorCode ERROR_CODE
	HighWatermark int64
	LastStableOffset int64
	LogStartOffset int64
	AbortedTransactions []FetchResponsePartitionAbortedTransaction
	PreferredReadReplica int32
	Records []byte
}

type FetchResponseTopic struct {
	TopicId UUID
	Partitions []FetchResponsePartition
}

type FetchResponseBody struct {
	ErrorCode ERROR_CODE
	ThrottleTimeMs int32
	SessionId int32
	Responses []FetchResponseTopic
}

type FetchRequestTopic struct {
	TopicId UUID
	Partitions []FetchRequestPartition
}

type FetchRequestPartition struct {
	Partition int32
	CurrentLeaderEpoch int32
	FetchOffset int64
	LastFetchedEpoch int32
	LogStartOffset int64
	PartitionMaxBytes int32
}

type FetchRequestForgettenTopic struct {
	TopicId UUID
	Partitions []int32
}

type FetchRequestBody struct {
	MaxWaitTimeMs int32
	MinBytes int32
	MaxBytes int32
	IsolationLevel int8
	SessionId int32
	SessionEpoch int32
	Topics []FetchRequestTopic
	ForgettenTopic []FetchRequestForgettenTopic
	RackId string
}

func parseFetchRequestBody(body []byte) (*FetchRequestBody, error) {
	reader := NewKafkaReader(body)
	
	maxWaitTimeMs, err := reader.Int32()
	if err != nil {
		return nil, err
	}
	minBytes, err := reader.Int32()
	if err != nil {
		return nil, err
	}
	MaxBytes, err := reader.Int32()
	if err != nil {
		return nil, err
	}
	IsolationLevel, err := reader.Int8()
	if err != nil {
		return nil, err
	}
	SessionId, err := reader.Int32()
	if err != nil {
		return nil, err
	}
	SessionEpoch, err := reader.Int32()
	if err != nil {
		return nil, err
	}
	// topics
	topicsLength, err := reader.Int8()
	if err != nil {
		return nil, err		
	}
	topics := []FetchRequestTopic{}
	if topicsLength >= 1{
		topicsLength--
		topics = make([]FetchRequestTopic, int(topicsLength))
		for i := 0; i < int(topicsLength); i++ {
			topicId, err := reader.Bytes(16)
			if err != nil {
				return nil, err
			}
			partitionsLength, err := reader.Int8()
			if err != nil {
				return nil, err
			}
			partitions := []FetchRequestPartition{}
			if partitionsLength >= 1{
				partitionsLength--
				partitions = make([]FetchRequestPartition, int(partitionsLength))
				for j := 0; j < int(partitionsLength); j++ {
					partition, err := reader.Int32()
					if err != nil {
						return nil, err
					}
					currentLeaderEpoch, err := reader.Int32()
					if err != nil {
						return nil, err
					}
					fetchOffset, err := reader.Int64()
					if err != nil {
						return nil, err
					}
					lastFetchedEpoch, err := reader.Int32()
					if err != nil {
						return nil, err
					}
					logStartOffset, err := reader.Int64()
					if err != nil {
						return nil, err
					}
					partitionMaxBytes, err := reader.Int32()
					if err != nil {
						return nil, err
					}
					partitions[j] = FetchRequestPartition{
						Partition: partition,
						CurrentLeaderEpoch: currentLeaderEpoch,
						FetchOffset: fetchOffset,
						LastFetchedEpoch: lastFetchedEpoch,
						LogStartOffset: logStartOffset,
						PartitionMaxBytes: partitionMaxBytes,
					}
					reader.SkipBytes(1) // tag buffer
				}
			}
			topics[i] = FetchRequestTopic{
				TopicId: UUID(topicId[:]),
				Partitions: partitions,
			}
			reader.SkipBytes(1) // tag buffer
		}
	}

	// forgetten topic
	forgettenTopicLength, err := reader.Int8()
	if err != nil {
		return nil, err
	}
	forgettenTopicLength--;
	forgettenTopic := make([]FetchRequestForgettenTopic, int(forgettenTopicLength))
	for i := 0; i < int(forgettenTopicLength); i++ {
		forgettenTopicId, err := reader.Bytes(16)
		if err != nil {
			return nil, err
		}
		partitionsLength, err := reader.Int8()
		if err != nil {
			return nil, err
		}
		partitions := []int32{}
		if partitionsLength >= 1{
			partitionsLength--;
			partitions := make([]int32, int(partitionsLength))
			for j := 0; j < int(partitionsLength); j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, err
				}
				partitions[j] = partition
			}
		}
		forgettenTopic[i] = FetchRequestForgettenTopic{
			TopicId: UUID(forgettenTopicId[:]),
			Partitions: partitions,
		}
		reader.SkipBytes(1) // tag buffer
	}

	rackIdLength, err := reader.Int8()
	if err != nil {
		return nil, err
	}
	rackId := ""
	if rackIdLength >= 1{
		rackIdLength--
		rackIdBytes, err := reader.Bytes(int(rackIdLength))
		if err != nil {
			return nil, err
		}
		rackId = string(rackIdBytes)
	}

	return &FetchRequestBody{
		MaxWaitTimeMs: maxWaitTimeMs,
		MinBytes: minBytes,
		MaxBytes: MaxBytes,
		IsolationLevel: IsolationLevel,
		SessionId: SessionId,
		SessionEpoch: SessionEpoch,
		Topics: topics,
		ForgettenTopic: forgettenTopic,
		RackId: rackId,
	}, nil
}

func generateBytesFromFetchResponseBody(body *FetchResponseBody) []byte {
	writer := NewKafkaWriter()

	writer.Int32(body.ThrottleTimeMs)
	writer.Int16(body.ErrorCode)
	writer.Int32(body.SessionId)
	writer.Int8(int8(len(body.Responses) + 1))

	for _, response := range body.Responses {
		writer.Bytes(response.TopicId[:])
		writer.Int8(int8(len(response.Partitions) + 1))
		for _, partition := range response.Partitions {
			writer.Int32(partition.PartitionIndex)
			writer.Int16(partition.ErrorCode)

			writer.Int64(partition.HighWatermark)
			writer.Int64(partition.LastStableOffset)
			writer.Int64(partition.LogStartOffset)

			writer.Int8(int8(len(partition.AbortedTransactions) + 1))
			for _, abortedTransaction := range partition.AbortedTransactions {
				writer.Int64(abortedTransaction.ProducerId)
				writer.Int64(abortedTransaction.FirstOffset)
				writer.Int8(0) // tag buffer
			}

			writer.Int32(partition.PreferredReadReplica)
			writer.Int8(int8(len(partition.Records)+1))
			writer.Bytes(partition.Records)

			writer.Int8(0) // tag buffer
		}
		writer.Int8(0) // tag buffer
	}

	writer.Int8(0) // tag buffer

	return writer.Build()
}

func handleFetchRequest(req *Request) *Response {
	requestBody, err := parseFetchRequestBody(req.Body)
	if err != nil {
		return nil
	}

	res := Response{
		CorrelationId: req.CorrelationId,
		HeaderVersion: 1,
	}

	responses := []FetchResponseTopic{}
	for _, topic := range requestBody.Topics {
		_, ok := topicIdToTopicName[topic.TopicId]
		errorCode := ERROR_CODE_NONE
		if !ok {
			errorCode = ERROR_CODE_UNKNOWN_TOPIC_ID
			responses = append(responses, FetchResponseTopic{
				TopicId: topic.TopicId,
				Partitions: []FetchResponsePartition{
					{
						PartitionIndex: 0,
						ErrorCode: errorCode,
					},
				},
			})
			continue
		}

		partitionIds, ok := topicIdToPartitionIds[topic.TopicId]
		if !ok {
			responses = append(responses, FetchResponseTopic{
				TopicId: topic.TopicId,
				Partitions: []FetchResponsePartition{},
			})
			continue
		}

		partitions := []FetchResponsePartition{}
		for _, partition := range topic.Partitions {
			partitionId := partition.Partition
			hasPartition := slices.Contains(partitionIds, partitionId)
			if !hasPartition {
				partitions = append(partitions, FetchResponsePartition{
					PartitionIndex: partitionId,
					ErrorCode: ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
				})
				continue
			}

			records, err := getTopicPartitionRecords(topic.TopicId, partitionId)
			if err != nil {
				partitions = append(partitions, FetchResponsePartition{
					PartitionIndex: partitionId,
					ErrorCode: ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
				})
				continue
			}
			partitions = append(partitions, FetchResponsePartition{
				PartitionIndex: partitionId,
				ErrorCode: errorCode,
				Records: records,
			})
		}
		responses = append(responses, FetchResponseTopic{
			TopicId: topic.TopicId,
			Partitions: partitions,
		})
	}

	responseBody := FetchResponseBody{
		ErrorCode: ERROR_CODE_NONE,
		ThrottleTimeMs: 0,
		SessionId: requestBody.SessionId,
		Responses: responses,
	}

	responseBodyBytes := generateBytesFromFetchResponseBody(&responseBody)
	res.Body = responseBodyBytes

	return &res
}