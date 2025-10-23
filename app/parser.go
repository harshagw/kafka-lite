package main

import (
	"fmt"
	"os"
)

func parseRequest(reqData []byte) (*Request, error) {
	if len(reqData) < 14 {
		return nil, fmt.Errorf("request data too short: expected at least 14 bytes, got %d", len(reqData))
	}

	reader := NewKafkaReader(reqData)
	req := Request{}

	messageSize, err := reader.Int32()
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}
	req.MessageSize = int32(messageSize)

	requestApiKey, err := reader.Int16()
	if err != nil {
		return nil, fmt.Errorf("failed to read request API key: %w", err)
	}
	req.RequestApiKey = int16(requestApiKey)

	requestApiVersion, err := reader.Int16()
	if err != nil {
		return nil, fmt.Errorf("failed to read request API version: %w", err)
	}
	req.RequestApiVersion = int16(requestApiVersion)

	correlationId, err := reader.Int32()
	if err != nil {
		return nil, fmt.Errorf("failed to read correlation ID: %w", err)
	}
	req.CorrelationId = int32(correlationId)

	clientIdLength, err := reader.Int16()
	if err != nil {
		return nil, fmt.Errorf("failed to read client ID length: %w", err)
	}

	req.ClientId, err = reader.Bytes(int(clientIdLength))
	if err != nil {
		return nil, fmt.Errorf("failed to read client ID: %w", err)
	}

	// Skip tag buffer (1 byte)
	if err := reader.SkipBytes(1); err != nil {
		return nil, fmt.Errorf("failed to skip tag buffer: %w", err)
	}

	req.Body, err = reader.RemainingBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	return &req, nil
}

func encodeResponse(res *Response) []byte {
	if res == nil {
		fmt.Println("Error sending response: response is nil")
		os.Exit(1)
	}

	bodySize := len(res.Body)
	headerSize := 4 // correlation_id
	if res.HeaderVersion == 1 {
		headerSize += 1 // tag_buffer
	}

	// Clean and simple response building
	writer := NewKafkaWriter()
	writer.Int32(int32(bodySize + headerSize))
	writer.Int32(res.CorrelationId)
	
	// Add tag buffer if needed
	if res.HeaderVersion == 1 {
		writer.Int8(0)
	}
	
	// Add response body
	if bodySize > 0 {
		writer.Bytes(res.Body)
	}
	
	return writer.Build()
}
