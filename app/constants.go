package main

const NULL_UINT8 = uint8(0xff)

var NULL_UUID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

var SUPPORTED_APIS = []SupportedAPIs{
	{ApiKey: 18, MinAPIVersion: 0, MaxAPIVersion: 4, ApiName: "ApiVersions"},
	{ApiKey: 75, MinAPIVersion: 0, MaxAPIVersion: 0, ApiName: "DescribeTopicPartitions"},
}