package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

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

func parseRequest(reqData []byte) Request {
	req := Request{}
	req.MessageSize = int32(binary.BigEndian.Uint32(reqData[0:4]))
	req.RequestApiKey = int16(binary.BigEndian.Uint16(reqData[4:6]))
	req.RequestApiVersion = int16(binary.BigEndian.Uint16(reqData[6:8]))
	req.CorrelationId = int32(binary.BigEndian.Uint32(reqData[8:12]))
	req.Body = reqData[12:]

	return req
}	

func sendResponse(conn net.Conn, res Response) {
	bodySize := int32(len(res.Body)) 
	resData := make([]byte, bodySize+8) // body(variable) + correlation id(4 bytes) + message_size (4 bytes)
	binary.BigEndian.PutUint32(resData[0:4], uint32(bodySize))
	binary.BigEndian.PutUint32(resData[4:8], uint32(res.CorrelationId))
	copy(resData[8:], res.Body)
	_, err := conn.Write(resData)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}
}

type SupportedAPIs struct {
	ApiKey int16
	MinAPIVersion int16
	MaxAPIVersion int16
	ApiName string
}

var supportedAPIs = []SupportedAPIs{
	{ApiKey: 18, MinAPIVersion: 0, MaxAPIVersion: 4, ApiName: "ApiVersions"},
}

func handleApiVersionsRequest(req Request) Response {
	res := Response{
		CorrelationId: req.CorrelationId,
	}

	var errorCode int16 = 0

	if req.RequestApiVersion < 0 || req.RequestApiVersion > 4 {
		errorCode = 35
	}

	responseBodyLength := 2 + 1 + 3 + (len(supportedAPIs) * 7) // 2 (error_code) + 1 (array_length) + 3 (throttle + tag_buffer) + 6 for each api (2 bytes for api_key, 2 bytes for min_api_version, 2 bytes for max_api_version, 1 byte for tag_buffers)

	fmt.Println("Response body length: ", responseBodyLength)

	responseBody := make([]byte, responseBodyLength)
	binary.BigEndian.PutUint16(responseBody[0:2], uint16(errorCode))
	responseBody[2] = uint8(len(supportedAPIs)+1) // For 0 supported APIs, we send 1 and so on
	
	offset := 3

	for _, api := range supportedAPIs {
		binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(api.ApiKey))
		offset += 2
		binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(api.MinAPIVersion))
		offset += 2
		binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(api.MaxAPIVersion))
		offset += 2
		responseBody[offset] = 0 // tag_buffer
		offset += 1
	}

	binary.BigEndian.PutUint16(responseBody[offset:offset+2], uint16(0)) // throttle
	offset += 2
	responseBody[offset] = 0 // tag_buffer
	offset += 1

	res.Body = responseBody
	return res
}


func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	reqData := make([]byte, 1024) 
	n, err := conn.Read(reqData)
	if err != nil {
		fmt.Println("Error reading data: ", err.Error())
		os.Exit(1)
	}
	reqData = reqData[:n] 
	req := parseRequest(reqData)

	res := Response{}
	switch req.RequestApiKey {
	case 18: // ApiVersions
		res = handleApiVersionsRequest(req)
	default:
		fmt.Println("Unknown API key: ", req.RequestApiKey)
		os.Exit(1)
	}

	sendResponse(conn, res)
}
