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
	HeaderVersion int
	Body          []byte
}

type SupportedAPIs struct {
	ApiKey        int16
	MinAPIVersion int16
	MaxAPIVersion int16
	ApiName       string
}

