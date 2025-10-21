package main

func handleApiVersionsRequest(req *Request) *Response {
	res := Response{
		CorrelationId: req.CorrelationId,
		Version:       0,
	}

	var errorCode int16 = 0

	if req.RequestApiVersion < 0 || req.RequestApiVersion > 4 {
		errorCode = 35
	}

	responseBodyLength := 2 + 1 + 5 + (len(SUPPORTED_APIS) * 7) // 2 (error_code) + 1 (array_length) + 5 (4 bytes for throttle + 1 byte for tag_buffer) + 7 for each api (2 bytes for api_key, 2 bytes for min_api_version, 2 bytes for max_api_version, 1 byte for tag_buffers)

	buf := NewWriteByteBuffer(&responseBodyLength)

	buf.WriteUint16(uint16(errorCode))
	buf.WriteUint8(uint8(len(SUPPORTED_APIS) + 1)) // For 0 supported APIs, we send 1 and so on

	for _, api := range SUPPORTED_APIS {
		buf.WriteUint16(uint16(api.ApiKey))
		buf.WriteUint16(uint16(api.MinAPIVersion))
		buf.WriteUint16(uint16(api.MaxAPIVersion))
		buf.WriteUint8(0) // tag_buffer
	}

	buf.WriteUint32(uint32(0)) // throttle
	buf.WriteUint8(0)          // tag_buffer

	res.Body = buf.Bytes()
	return &res
}
