package main

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/ktypes"
)

type SupportedAPIsKType struct {
	ApiKey        ktypes.Int16  `order:"1"`
	MinAPIVersion ktypes.Int16  `order:"2"`
	MaxAPIVersion ktypes.Int16  `order:"3"`
	ApiName       ktypes.String `order:"4"`
}

type ApiVersionsResponseBody struct {
	ErrorCode      ERROR_CODE  `order:"1"`
	ApiVersions    ktypes.Array[SupportedAPIsKType] `order:"2"`
	ThrottleTimeMs ktypes.Int32  `order:"3"`
}

func generateBytesFromApiVersionsResponseBody(body *ApiVersionsResponseBody) []byte {
	encoder := ktypes.NewKEncoder()
	encoded, err := encoder.Encode(body)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode API versions response: %v", err))
	}
	return encoded
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

	apiVersions := []SupportedAPIsKType{
		{ApiKey: ktypes.Int16(API_VERSIONS_REQUEST_KEY), MinAPIVersion: ktypes.Int16(0), MaxAPIVersion: ktypes.Int16(4), ApiName: ktypes.String("ApiVersions")},
		{ApiKey: ktypes.Int16(DESCRIBE_TOPIC_PARTITIONS_REQUEST_KEY), MinAPIVersion: ktypes.Int16(0), MaxAPIVersion: ktypes.Int16(0), ApiName: ktypes.String("DescribeTopicPartitions")},
		{ApiKey: ktypes.Int16(FETCH_REQUEST_KEY), MinAPIVersion: ktypes.Int16(0), MaxAPIVersion: ktypes.Int16(16), ApiName: ktypes.String("Fetch")},
		{ApiKey: ktypes.Int16(PRODUCE_REQUEST_KEY), MinAPIVersion: ktypes.Int16(0), MaxAPIVersion: ktypes.Int16(11), ApiName: ktypes.String("Produce")},
	}

	responseBody := ApiVersionsResponseBody{
		ErrorCode:      errorCode,
		ApiVersions:    apiVersions,
		ThrottleTimeMs: ktypes.Int32(0),
	}

	responseBodyBytes := generateBytesFromApiVersionsResponseBody(&responseBody)

	res.Body = responseBodyBytes
	return &res
}
