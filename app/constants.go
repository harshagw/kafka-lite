package main

import "github.com/codecrafters-io/kafka-starter-go/app/ktypes"

var NULL_UUID = ktypes.UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

const (
	API_VERSIONS_REQUEST_KEY              = 18
	DESCRIBE_TOPIC_PARTITIONS_REQUEST_KEY = 75
	FETCH_REQUEST_KEY                      = 1
	PRODUCE_REQUEST_KEY                    = 0
)

type ERROR_CODE = ktypes.Int16

const (
	ERROR_CODE_NONE                       ERROR_CODE = 0
	ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION ERROR_CODE = 3
	ERROR_CODE_UNKNOWN_TOPIC_ID           ERROR_CODE = 100
	ERROR_CODE_UNSUPPORTED_VERSION        ERROR_CODE = 35
)

const METADATA_TOPIC = "__clusters_metadata"
const LOGS_BASE_FOLDER = "/tmp/kraft-combined-logs/"
const TEST_LOG_FILE = "tmp/kraft-combined-logs/__cluster_metadata-0/kafka_sample.log"
const LOGS_SRC_FOLDER = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
