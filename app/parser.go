package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/ktypes"
)

func parseRequest(reqData []byte) (*Request, error) {
	if len(reqData) < 14 {
		return nil, fmt.Errorf("request data too short: expected at least 14 bytes, got %d", len(reqData))
	}

	decoder := ktypes.NewKDecoder(reqData)
	var req Request
	err := decoder.Decode(&req)
	if err != nil {
		return nil, fmt.Errorf("failed to decode request: %w", err)
	}
	req.Body = make([]byte, decoder.RemainingBytes())

	return &req, nil
}

func encodeResponse(res *Response) []byte {
	if res == nil {
		fmt.Println("Error sending response: response is nil")
		os.Exit(1)
	}

	encoder := ktypes.NewKEncoder()
	encoded, err := encoder.Encode(res)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode response: %v", err))
	}
	return encoded
}
