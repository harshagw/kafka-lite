package main


type ApiVersionsResponseBody struct {
	ErrorCode ERROR_CODE
	ApiVersions []SupportedAPIs
	ThrottleTimeMs int32
}

func generateBytesFromApiVersionsResponseBody(body *ApiVersionsResponseBody) []byte {
	writer := NewKafkaWriter()

	writer.Int16(body.ErrorCode)
	writer.Int8(int8(len(body.ApiVersions) + 1))
	
	for _, api := range body.ApiVersions {
		writer.Int16(api.ApiKey)
		writer.Int16(api.MinAPIVersion)
		writer.Int16(api.MaxAPIVersion)
		writer.Int8(0) // tag_buffer
	}

	writer.Int32(body.ThrottleTimeMs)
	writer.Int8(0) // tag_buffer
	
	return writer.Build()
}

func handleApiVersionsRequest(req *Request) *Response {
	res := Response{
		CorrelationId: req.CorrelationId,
		HeaderVersion: 0,
	}

	var errorCode ERROR_CODE = ERROR_CODE_NONE

	if req.RequestApiVersion < 0 || req.RequestApiVersion > 4 {
		errorCode = ERROR_CODE_UNSUPPORTED_VERSION
	}

	responseBody := ApiVersionsResponseBody{
		ErrorCode: errorCode,
		ApiVersions: SUPPORTED_APIS,
		ThrottleTimeMs: 0,
	}

	responseBodyBytes := generateBytesFromApiVersionsResponseBody(&responseBody)

	res.Body = responseBodyBytes
	return &res
}
