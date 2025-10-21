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
	MessageSize int32
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
	responseSize := int32(len(res.Body)) + 8
	resData := make([]byte, responseSize)
	binary.BigEndian.PutUint32(resData[0:4], uint32(res.MessageSize))
	binary.BigEndian.PutUint32(resData[4:8], uint32(res.CorrelationId))
	copy(resData[8:], res.Body)
	_, err := conn.Write(resData)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}
}

func handleApiVersionsRequest(req Request) Response {
	res := Response{
		MessageSize: req.MessageSize,
		CorrelationId: req.CorrelationId,
	}

	var errorCode int16 = 0

	if req.RequestApiVersion < 0 || req.RequestApiVersion >= 4 {
		errorCode = 35
	}

	responseBody := make([]byte, 2)
	binary.BigEndian.PutUint16(responseBody[0:2], uint16(errorCode))
	
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
