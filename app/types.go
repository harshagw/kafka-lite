package main

type Request struct {
	MessageSize int32
	RequestApiKey int16
	RequestApiVersion int16
	CorrelationId int32
	Body []byte
}

type Response struct {
	CorrelationId int32
	Body []byte
}

type SupportedAPIs struct {
	ApiKey int16
	MinAPIVersion int16
	MaxAPIVersion int16
	ApiName string
}

var supportedAPIs = []SupportedAPIs{
	{ApiKey: 18, MinAPIVersion: 0, MaxAPIVersion: 4, ApiName: "ApiVersions"},
	{ApiKey: 75, MinAPIVersion: 0, MaxAPIVersion: 0, ApiName: "DescribeTopicPartitions"},
}

