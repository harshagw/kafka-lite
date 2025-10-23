package main

type FetchResponseBody struct {
	ErrorCode ERROR_CODE
	ThrottleTimeMs int32
	SessionId int32
	Topics []string
}

func generateBytesFromFetchResponseBody(body *FetchResponseBody) []byte {
	writer := NewKafkaWriter()

	writer.Int32(body.ThrottleTimeMs)
	writer.Int16(body.ErrorCode)
	writer.Int32(body.SessionId)
	writer.Int8(int8(len(body.Topics) + 1))

	writer.Int8(0) // tag buffer

	return writer.Build()
}

func handleFetchRequest(req *Request) *Response {
	res := Response{
		CorrelationId: req.CorrelationId,
		HeaderVersion: 1,
	}

	responseBody := FetchResponseBody{
		ErrorCode: ERROR_CODE_NONE,
		ThrottleTimeMs: 0,
		SessionId: 0,
		Topics: []string{},
	}

	responseBodyBytes := generateBytesFromFetchResponseBody(&responseBody)
	res.Body = responseBodyBytes

	return &res
}