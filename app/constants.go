package main

type UUID [16]byte

var NULL_UUID = UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

const (
	API_VERSIONS_REQUEST_KEY              = 18
	DESCRIBE_TOPIC_PARTITIONS_REQUEST_KEY = 75
)

var SUPPORTED_APIS = []SupportedAPIs{
	{ApiKey: API_VERSIONS_REQUEST_KEY, MinAPIVersion: 0, MaxAPIVersion: 4, ApiName: "ApiVersions"},
	{ApiKey: DESCRIBE_TOPIC_PARTITIONS_REQUEST_KEY, MinAPIVersion: 0, MaxAPIVersion: 0, ApiName: "DescribeTopicPartitions"},
}

type ERROR_CODE = int16

const (
	ERROR_CODE_NONE                       ERROR_CODE = 0
	ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION ERROR_CODE = 3
	ERROR_CODE_UNSUPPORTED_VERSION        ERROR_CODE = 35
)

const METADATA_TOPIC = "__clusters_metadata"

const LOGS_SRC_FOLDER = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
