package main

type Request struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          []byte
	Body              []byte
}

type Response struct {
	CorrelationId int32
	Version       int
	Body          []byte
}

type SupportedAPIs struct {
	ApiKey        int16
	MinAPIVersion int16
	MaxAPIVersion int16
	ApiName       string
}

type AuthorizedOperation = uint8

// enum for authorized operations
const (
	AUTHORIZED_OPERATION_UNKNOWN          AuthorizedOperation = iota
	AUTHORIZED_OPERATION_ANY              AuthorizedOperation = 1
	AUTHORIZED_OPERATION_ALL              AuthorizedOperation = 2
	AUTHORIZED_OPERATION_READ             AuthorizedOperation = 3
	AUTHORIZED_OPERATION_WRITE            AuthorizedOperation = 4
	AUTHORIZED_OPERATION_CREATE           AuthorizedOperation = 5
	AUTHORIZED_OPERATION_DELETE           AuthorizedOperation = 6
	AUTHORIZED_OPERATION_ALTER            AuthorizedOperation = 7
	AUTHORIZED_OPERATION_DESCRIBE         AuthorizedOperation = 8
	AUTHORIZED_OPERATION_CLUSTER_ACTION   AuthorizedOperation = 9
	AUTHORIZED_OPERATION_DESCRIBE_CONFIGS AuthorizedOperation = 10
	AUTHORIZED_OPERATION_ALTER_CONFIGS    AuthorizedOperation = 11
	AUTHORIZED_OPERATION_IDEMPOTENT_WRITE AuthorizedOperation = 12
	AUTHORIZED_OPERATION_CREATE_TOKENS    AuthorizedOperation = 13
	AUTHORIZED_OPERATION_DESCRIBE_TOKENS  AuthorizedOperation = 14
)

type Topic struct {
	Name                 string
	ID                   uint32
	IsInternal           bool
	Partitions           []string
	AuthorizedOperations []AuthorizedOperation
}
