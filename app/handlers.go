package main

import (
	"encoding/binary"
)

func handleApiVersionsRequest(req Request) Response {
	res := Response{
		CorrelationId: req.CorrelationId,
	}

	var errorCode int16 = 0

	if req.RequestApiVersion < 0 || req.RequestApiVersion > 4 {
		errorCode = 35
	}

	responseBodyLength := 2 + 1 + 5 + (len(supportedAPIs) * 7) // 2 (error_code) + 1 (array_length) + 5 (4 bytes for throttle + 1 byte for tag_buffer) + 7 for each api (2 bytes for api_key, 2 bytes for min_api_version, 2 bytes for max_api_version, 1 byte for tag_buffers)

	responseBody := make([]byte, responseBodyLength)
	binary.BigEndian.PutUint16(responseBody[0:2], uint16(errorCode))
	responseBody[2] = uint8(len(supportedAPIs)+1) // For 0 supported APIs, we send 1 and so on
	
	offset := 3

	for _, api := range supportedAPIs {
		binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(api.ApiKey))
		offset += 2
		binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(api.MinAPIVersion))
		offset += 2
		binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(api.MaxAPIVersion))
		offset += 2
		responseBody[offset] = 0 // tag_buffer
		offset += 1
	}

	binary.BigEndian.PutUint32(responseBody[offset:offset+4], uint32(0)) // throttle
	offset += 4
	responseBody[offset] = 0 // tag_buffer
	offset += 1

	res.Body = responseBody
	return res
}
