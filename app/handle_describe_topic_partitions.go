package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/codecrafters-io/kafka-starter-go/app/ktypes"
)

type DescribeTopicPartitionsRequestBody struct {
	Topics                 ktypes.CompactArray[ktypes.CompactString] `order:"1"`
	ResponsePartitionLimit ktypes.Int32                             `order:"2"`
	Cursor                 ktypes.Int8                              `order:"3"`
}

type DescribeTopicPartitionsResponsePartition struct {
	ErrorCode              ERROR_CODE  `order:"1"`
	Index                  ktypes.Int32  `order:"2"`
	LeaderId               ktypes.Int32  `order:"3"`
	LeaderEpoch            ktypes.Int32  `order:"4"`
	ReplicaNodes           ktypes.CompactArray[ktypes.Int32] `order:"5"`
	ISRNodes               ktypes.CompactArray[ktypes.Int32] `order:"6"`
	EligibleLeaderReplicas ktypes.CompactArray[ktypes.Int32] `order:"7"`
	LastKnownELR           ktypes.CompactArray[ktypes.Int32] `order:"8"`
	OfflineReplicas        ktypes.CompactArray[ktypes.Int32] `order:"9"`
}

type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode                 ERROR_CODE  `order:"1"`
	Name                      ktypes.CompactString `order:"2"`
	Id                        ktypes.UUID   `order:"3"`
	IsInternal                ktypes.Bool   `order:"4"`
	Partitions                ktypes.CompactArray[DescribeTopicPartitionsResponsePartition] `order:"5"`
	TopicAuthorizedOperations ktypes.Int32  `order:"6"`
}

type DescribeTopicPartitionsResponseBody struct {
	ThrottleTimeMs ktypes.Int32 `order:"1"`
	Topics         ktypes.CompactArray[DescribeTopicPartitionsResponseTopic] `order:"2"`
	Cursor         ktypes.Int8  `order:"3"`
}

func generateBytesFromDescribeTopicPartitionsResponseBody(body *DescribeTopicPartitionsResponseBody) []byte {
	encoder := ktypes.NewKEncoder()
	encoded, err := encoder.Encode(body)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode describe topic partitions response: %v", err))
	}
	return encoded
}

func parseDescribeTopicPartitionsRequestBody(body []byte) (*DescribeTopicPartitionsRequestBody, error) {
	decoder := ktypes.NewKDecoder(body)
	var requestBody DescribeTopicPartitionsRequestBody
	err := decoder.Decode(&requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode describe topic partitions request: %v", err)
	}
	
	slices.SortFunc(requestBody.Topics, func(a, b ktypes.CompactString) int {
		return strings.Compare(string(a), string(b))
	})
	
	return &requestBody, nil
}

func handleDescribeTopicPartitionsRequest(req *Request) *Response {
	requestBody, err := parseDescribeTopicPartitionsRequestBody(req.Body)
	if err != nil {
		return nil
	}

	res := Response{
		CorrelationId: req.CorrelationId,
		HeaderVersion: 1,
	}

	responseBody := DescribeTopicPartitionsResponseBody{
		ThrottleTimeMs: ktypes.Int32(0),
		Topics: func() []DescribeTopicPartitionsResponseTopic {
			topics := make([]DescribeTopicPartitionsResponseTopic, len(requestBody.Topics))
			for i := range requestBody.Topics {
				topicName := string(requestBody.Topics[i])
				topicId := topicNameToTopicId[topicName]
				errorCode := ERROR_CODE_NONE
				if topicId == NULL_UUID {
					errorCode = ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
				}
				
				// Convert partitions to ktypes
				partitions := make([]DescribeTopicPartitionsResponsePartition, len(topicIdToPartitions[topicId]))
				for j := range topicIdToPartitions[topicId] {
					partition := topicIdToPartitions[topicId][j]
					
					// Convert slices to ktypes arrays
					replicaNodes := make([]ktypes.Int32, len(partition.Replicas))
					for k, replica := range partition.Replicas {
						replicaNodes[k] = ktypes.Int32(replica)
					}
					
					isrNodes := make([]ktypes.Int32, len(partition.InSyncReplicas))
					for k, isr := range partition.InSyncReplicas {
						isrNodes[k] = ktypes.Int32(isr)
					}
					
					partitions[j] = DescribeTopicPartitionsResponsePartition{
						ErrorCode:              ERROR_CODE_NONE,
						Index:                  ktypes.Int32(partition.PartitionId),
						LeaderId:               ktypes.Int32(partition.Leader),
						LeaderEpoch:            ktypes.Int32(partition.LeaderEpoch),
						ReplicaNodes:           replicaNodes,
						ISRNodes:               isrNodes,
						EligibleLeaderReplicas: []ktypes.Int32{}, // empty for now
						LastKnownELR:           []ktypes.Int32{}, // empty for now
						OfflineReplicas:        []ktypes.Int32{}, // empty for now
					}
				}
				
				topics[i] = DescribeTopicPartitionsResponseTopic{
					ErrorCode:                 errorCode,
					Name:                      ktypes.CompactString(topicName),
					Id:                        ktypes.UUID(topicId),
					IsInternal:                ktypes.Bool(false),
					Partitions:                partitions,
					TopicAuthorizedOperations: ktypes.Int32(0), // for now, lets this be some constant value
				}
			}
			return topics
		}(),
		Cursor: ktypes.Int8(-1), // null for now
	}

	res.Body = generateBytesFromDescribeTopicPartitionsResponseBody(&responseBody)
	return &res
}
