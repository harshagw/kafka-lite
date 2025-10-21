package main

import (
	"fmt"
	"net"
	"os"
)

func parseRequest(reqData []byte) (*Request, error) {
	if len(reqData) < 14 {
		return nil, fmt.Errorf("request data too short: expected at least 14 bytes, got %d", len(reqData))
	}

	buf := NewByteBuffer(reqData)
	req := Request{}

	messageSize, err := buf.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}
	req.MessageSize = int32(messageSize)

	requestApiKey, err := buf.ReadUint16()
	if err != nil {
		return nil, fmt.Errorf("failed to read request API key: %w", err)
	}
	req.RequestApiKey = int16(requestApiKey)

	requestApiVersion, err := buf.ReadUint16()
	if err != nil {
		return nil, fmt.Errorf("failed to read request API version: %w", err)
	}
	req.RequestApiVersion = int16(requestApiVersion)

	correlationId, err := buf.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read correlation ID: %w", err)
	}
	req.CorrelationId = int32(correlationId)

	clientIdLength, err := buf.ReadUint16()
	if err != nil {
		return nil, fmt.Errorf("failed to read client ID length: %w", err)
	}

	req.ClientId, err = buf.ReadBytes(int(clientIdLength))
	if err != nil {
		return nil, fmt.Errorf("failed to read client ID: %w", err)
	}

	// Skip tag buffer (1 byte)
	if err := buf.SkipBytes(1); err != nil {
		return nil, fmt.Errorf("failed to skip tag buffer: %w", err)
	}

	req.Body, err = buf.ReadRemaining()
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	return &req, nil
}

func sendResponse(conn net.Conn, res *Response) {
	if res == nil {
		fmt.Println("Error sending response: response is nil")
		os.Exit(1)
	}

	bodySize := len(res.Body)
	headerSize := 4 // correlation_id
	if res.Version > 0 {
		headerSize += 1 // tag_buffer
	}

	bufferSize := bodySize + headerSize + 4 // body + header + messageSize
	buf := NewWriteByteBuffer(&bufferSize)

	// Write message size
	buf.WriteUint32(uint32(bodySize + headerSize))

	// Write correlation ID
	buf.WriteUint32(uint32(res.CorrelationId))

	// Write tag buffer
	if res.Version > 0 {
		buf.WriteUint8(0) // tag_buffer
	}

	// Write response body
	if bodySize > 0 {
		buf.WriteBytes(res.Body)
	}

	// Send the response
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}
}
