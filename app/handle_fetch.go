package main

import (
	"fmt"
	"slices"

	"github.com/codecrafters-io/kafka-starter-go/app/ktypes"
)

type FetchResponsePartitionAbortedTransaction struct {
	ProducerId  ktypes.Int64 `order:"1"`
	FirstOffset ktypes.Int64 `order:"2"`
}

type FetchResponsePartition struct {
	PartitionIndex       ktypes.Int32  `order:"1"`
	ErrorCode            ERROR_CODE    `order:"2"`
	HighWatermark        ktypes.Int64  `order:"3"`
	LastStableOffset     ktypes.Int64  `order:"4"`
	LogStartOffset       ktypes.Int64  `order:"5"`
	AbortedTransactions  ktypes.CompactArray[FetchResponsePartitionAbortedTransaction] `order:"6"`
	PreferredReadReplica ktypes.Int32  `order:"7"`
	Records              ktypes.CompactRecords `order:"8"`
}

type FetchResponseTopic struct {
	TopicId    ktypes.UUID `order:"1"`
	Partitions ktypes.CompactArray[FetchResponsePartition] `order:"2"`
}

type FetchResponseBody struct {
	ErrorCode      ERROR_CODE `order:"1"`
	ThrottleTimeMs ktypes.Int32 `order:"2"`
	SessionId      ktypes.Int32 `order:"3"`
	Responses      ktypes.CompactArray[FetchResponseTopic] `order:"4"`
}

type FetchRequestTopic struct {
	TopicId    ktypes.UUID `order:"1"`
	Partitions ktypes.CompactArray[FetchRequestPartition] `order:"2"`
}

type FetchRequestPartition struct {
	Partition         ktypes.Int32 `order:"1"`
	CurrentLeaderEpoch ktypes.Int32 `order:"2"`
	FetchOffset       ktypes.Int64 `order:"3"`
	LastFetchedEpoch  ktypes.Int32 `order:"4"`
	LogStartOffset    ktypes.Int64 `order:"5"`
	PartitionMaxBytes ktypes.Int32 `order:"6"`
}

type FetchRequestForgettenTopic struct {
	TopicId    ktypes.UUID `order:"1"`
	Partitions ktypes.CompactArray[ktypes.Int32] `order:"2"`
}

type FetchRequestBody struct {
	MaxWaitTimeMs   ktypes.Int32 `order:"1"`
	MinBytes        ktypes.Int32 `order:"2"`
	MaxBytes        ktypes.Int32 `order:"3"`
	IsolationLevel  ktypes.Int8  `order:"4"`
	SessionId       ktypes.Int32 `order:"5"`
	SessionEpoch    ktypes.Int32 `order:"6"`
	Topics          ktypes.CompactArray[FetchRequestTopic] `order:"7"`
	ForgettenTopic  ktypes.CompactArray[FetchRequestForgettenTopic] `order:"8"`
	RackId          ktypes.CompactString `order:"9"`
}

func parseFetchRequestBody(body []byte) (*FetchRequestBody, error) {
	decoder := ktypes.NewKDecoder(body)
	var requestBody FetchRequestBody
	err := decoder.Decode(&requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode fetch request: %v", err)
	}
	return &requestBody, nil
}

func generateBytesFromFetchResponseBody(body *FetchResponseBody) []byte {
	encoder := ktypes.NewKEncoder()
	encoded, err := encoder.Encode(body)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode fetch response: %v", err))
	}
	return encoded
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
		topicId := ktypes.UUID(topic.TopicId)
		_, ok := topicIdToTopicName[topicId]
		if !ok {
			// Topic not found
			responses = append(responses, FetchResponseTopic{
				TopicId: topicId,
				Partitions: []FetchResponsePartition{
					{
						PartitionIndex: ktypes.Int32(0),
						ErrorCode: ERROR_CODE_UNKNOWN_TOPIC_ID,
					},
				},
			})
			continue
		}

		partitionIds, ok := topicIdToPartitionIds[topicId]
		if !ok {
			// Topic has no partitions
			responses = append(responses, FetchResponseTopic{
				TopicId: topicId,
				Partitions: []FetchResponsePartition{},
			})
			continue
		}

		partitions := []FetchResponsePartition{}
		for _, partition := range topic.Partitions {
			partitionId := int32(partition.Partition)
			hasPartition := slices.Contains(partitionIds, partitionId)
			if !hasPartition {
				// Partition not found
				partitions = append(partitions, FetchResponsePartition{
					PartitionIndex: ktypes.Int32(partitionId),
					ErrorCode: ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
				})
				continue
			}

			records, err := getTopicPartitionRecords(topicId, partitionId)
			if err != nil {
				// Error getting partition records
				partitions = append(partitions, FetchResponsePartition{
					PartitionIndex: ktypes.Int32(partitionId),
					ErrorCode: ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
				})
				continue
			}
			partitions = append(partitions, FetchResponsePartition{
				PartitionIndex: ktypes.Int32(partitionId),
				ErrorCode: ERROR_CODE_NONE,
				Records: ktypes.CompactRecords(records),
			})
		}
		responses = append(responses, FetchResponseTopic{
			TopicId: topicId,
			Partitions: partitions,
		})
	}

	responseBody := FetchResponseBody{
		ErrorCode: ERROR_CODE_NONE,
		ThrottleTimeMs: ktypes.Int32(0),
		SessionId: requestBody.SessionId,
		Responses: responses,
	}

	responseBodyBytes := generateBytesFromFetchResponseBody(&responseBody)
	res.Body = responseBodyBytes

	return &res
}