package main

import "sort"

type DescribeTopicPartitionsRequestBody struct {
	Topics                 []string
	ResponsePartitionLimit int32
	Cursor                 int8
}

type DescribeTopicPartitionsResponsePartition struct {
	ErrorCode              ERROR_CODE
	Index                  int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	ISRNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownELR           []int32
	OfflineReplicas        []int32
}

type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode                 ERROR_CODE
	Name                      string
	Id                        UUID
	IsInternal                bool
	Partitions                []DescribeTopicPartitionsResponsePartition
	TopicAuthorizedOperations int32
}

type DescribeTopicPartitionsResponseBody struct {
	ThrottleTimeMs int32
	Topics         []DescribeTopicPartitionsResponseTopic
	Cursor         int8
}

func generateBytesFromDescribeTopicPartitionsResponseBody(body *DescribeTopicPartitionsResponseBody) []byte {
	writer := NewKafkaWriter()

	writer.Int32(body.ThrottleTimeMs)
	writer.Int8(int8(len(body.Topics) + 1)) // Compact array of topics

	for _, topic := range body.Topics {
		writer.Int16(topic.ErrorCode)
		topicNameLength := len(topic.Name)
		writer.Int8(int8(topicNameLength) + 1)
		if topicNameLength > 0 {
			writer.Bytes([]byte(topic.Name))
		}

		writer.Bytes(topic.Id[:])

		if topic.IsInternal {
			writer.Int8(1) // is_internal as true
		} else {
			writer.Int8(0) // is_internal as false
		}

		writer.Int8(int8(len(topic.Partitions) + 1)) // Compact array of partitions

		for _, partition := range topic.Partitions {
			writer.Int16(partition.ErrorCode)
			writer.Int32(partition.Index)
			writer.Int32(partition.LeaderId)
			writer.Int32(partition.LeaderEpoch)
			writer.Int8(int8(len(partition.ReplicaNodes) + 1)) // Compact array of replica nodes
			for _, replicaNode := range partition.ReplicaNodes {
				writer.Int32(replicaNode)
			}
			writer.Int8(int8(len(partition.ISRNodes) + 1)) // Compact array of ISR nodes
			for _, isrNode := range partition.ISRNodes {
				writer.Int32(isrNode)
			}
			writer.Int8(int8(len(partition.EligibleLeaderReplicas) + 1)) // Compact array of eligible leader replicas
			for _, eligibleLeaderReplica := range partition.EligibleLeaderReplicas {
				writer.Int32(eligibleLeaderReplica)
			}
			writer.Int8(int8(len(partition.LastKnownELR) + 1)) // Compact array of last known ELR
			for _, lastKnownELR := range partition.LastKnownELR {
				writer.Int32(lastKnownELR)
			}
			writer.Int8(int8(len(partition.OfflineReplicas) + 1)) // Compact array of offline replicas
			for _, offlineReplica := range partition.OfflineReplicas {
				writer.Int32(offlineReplica)
			}
			writer.Int8(0) // tag_buffer
		}

		writer.Int32(topic.TopicAuthorizedOperations) // authorized operations
		writer.Int8(0) // tag_buffer
	}

	writer.Int8(body.Cursor) // next cursor
	writer.Int8(0)           // tag buffer

	return writer.Build()
}

func parseDescribeTopicPartitionsRequestBody(body []byte) (*DescribeTopicPartitionsRequestBody, error) {
	reader := NewKafkaReader(body)
	topicLength, err := reader.Int8()
	if err != nil {
		return nil, err
	}
	topicLength--
	topic := make([]string, topicLength)
	if topicLength > 0 {
		for i := 0; i < int(topicLength); i++ {
			topicNameLength, err := reader.Int8()
			if err != nil {
				return nil, err
			}
			topicNameLength--
			if topicNameLength > 0 {
				topicName, err := reader.Bytes(int(topicNameLength))
				if err != nil {
					return nil, err
				}
				topic[i] = string(topicName)
			}
			reader.SkipBytes(1) // topic buffer
		}
	}
	// sort topics alphabetically
	sort.Strings(topic)
	responsePartitionLimit, err := reader.Int32()
	if err != nil {
		return nil, err
	}
	cursor, err := reader.Int8()
	if err != nil {
		return nil, err
	}
	return &DescribeTopicPartitionsRequestBody{
		Topics:                 topic,
		ResponsePartitionLimit: responsePartitionLimit,
		Cursor:                 cursor,
	}, nil
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
		ThrottleTimeMs: 0,
		Topics: func() []DescribeTopicPartitionsResponseTopic {
			topics := make([]DescribeTopicPartitionsResponseTopic, len(requestBody.Topics))
			for i := range requestBody.Topics {
				topicId := topicNameToTopicId[requestBody.Topics[i]]
				errorCode := ERROR_CODE_NONE
				if topicId == NULL_UUID {
					errorCode = ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
				}
				topics[i] = DescribeTopicPartitionsResponseTopic{
					ErrorCode:                 errorCode,
					Name:                      requestBody.Topics[i],
					Id:                        topicId,
					IsInternal:                false,
					Partitions:                func() []DescribeTopicPartitionsResponsePartition {
						partitions := make([]DescribeTopicPartitionsResponsePartition, len(topicIdToPartitions[topicId]))
						for i := range topicIdToPartitions[topicId] {
							partition := topicIdToPartitions[topicId][i]
							partitions[i] = DescribeTopicPartitionsResponsePartition{
								ErrorCode:                 ERROR_CODE_NONE,
								Index:                     partition.PartitionId,
								LeaderId:                  partition.Leader,
								LeaderEpoch:               partition.LeaderEpoch,
								ReplicaNodes:              partition.Replicas,
								ISRNodes:                  partition.InSyncReplicas,
							}
						}
						return partitions
					}(),
					TopicAuthorizedOperations: int32(0), // for now, lets this be some constant value
				}
			}
			return topics
		}(),
		Cursor: int8(-1), // null for now
	}

	res.Body = generateBytesFromDescribeTopicPartitionsResponseBody(&responseBody)
	return &res
}
