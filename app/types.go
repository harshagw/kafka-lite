package main

import "github.com/codecrafters-io/kafka-starter-go/app/ktypes"

type Request struct {
	MessageSize       ktypes.Int32  `order:"1"`
	RequestApiKey     ktypes.Int16  `order:"2"`
	RequestApiVersion ktypes.Int16  `order:"3"`
	CorrelationId     ktypes.Int32  `order:"4"`
	ClientId          ktypes.String `order:"5"`
	Body              []byte
}

type Response struct {
	CorrelationId ktypes.Int32 `order:"1"`
	HeaderVersion ktypes.Int32 `order:"2"`
	Body          ktypes.Bytes `order:"3"`
}

