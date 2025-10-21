package main

import "math/big"

type DescribeTopicPartitionsRequestBody struct {
	Topics                  []string
	ResponsePartitionLimit uint32
	Cursor                 uint8
}

func parseDescribeTopicPartitionsRequestBody(body []byte) (*DescribeTopicPartitionsRequestBody, error) {
	buf := NewByteBuffer(body)
	topicLength, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}
	topicLength--
	topic := make([]string, topicLength)
	if topicLength > 0 {
		for i := 0; i < int(topicLength); i++ {
			topicNameLength, err := buf.ReadUint8()
			if err != nil {
				return nil, err
			}
			topicNameLength--
			if topicNameLength > 0 {
				topicName, err := buf.ReadBytes(int(topicNameLength))
				if err != nil {
					return nil, err
				}
				topic[i] = string(topicName)
			}
			buf.SkipBytes(1) // topic buffer
		}
	}
	responsePartitionLimit, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}
	cursor, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}
	return &DescribeTopicPartitionsRequestBody{
		Topics:                  topic,
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
		Version:       1,
	}

	buf := NewWriteByteBuffer(nil)

	buf.WriteUint32(0) // throttle time

	buf.WriteUint8(uint8(len(requestBody.Topics) + 1))

	// write the topics -- hardcoded until we have topics
	for _, topic := range requestBody.Topics {
		buf.WriteUint16(3) // UNKNOWN_TOPIC_OR_PARTITION Error code
		topicNameLength := len(topic)
		buf.WriteUint8(uint8(topicNameLength) + 1)
		if topicNameLength > 0 {
			buf.WriteBytes([]byte(topic))
		}
		
		buf.WriteBytes(NULL_UUID[:]) // id
		buf.WriteUint8(0) // is_internal as false
		buf.WriteUint8(1) // for 0 array of partition

		// for authorized operation
		bitArray := big.NewInt(0)
		bitArray.SetBit(bitArray, 0, 0) 
		bitArray.SetBit(bitArray, 1, 0)
		bitArray.SetBit(bitArray, 2, 0)
		bitArray.SetBit(bitArray, 3, 1)
		bitArray.SetBit(bitArray, 4, 1)
		bitArray.SetBit(bitArray, 5, 1)
		bitArray.SetBit(bitArray, 6, 1)
		bitArray.SetBit(bitArray, 7, 1)
		bitArray.SetBit(bitArray, 8, 1)
		bitArray.SetBit(bitArray, 9, 0)
		bitArray.SetBit(bitArray, 10, 1)
		bitArray.SetBit(bitArray, 11, 1)
		bitArray.SetBit(bitArray, 12, 0)
		bitArray.SetBit(bitArray, 13, 0)
		bitArray.SetBit(bitArray, 14, 0)
		bitArray.SetBit(bitArray, 15, 0)
		buf.WriteUint32(uint32(bitArray.Int64())) 

		buf.WriteUint8(0) // tag_buffer 
	}


	buf.WriteUint8(NULL_UINT8) // next cursor
	buf.WriteUint8(0)          // tag buffer
	res.Body = buf.Bytes()
	return &res
}
