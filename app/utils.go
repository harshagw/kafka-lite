package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

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
	messageSize := bodySize + 4 // bodySize + correlation_id size
	resData := make([]byte, bodySize+8) // body(variable) + correlation id(4 bytes) + message_size (4 bytes)
	binary.BigEndian.PutUint32(resData[0:4], uint32(messageSize))
	binary.BigEndian.PutUint32(resData[4:8], uint32(res.CorrelationId))
	copy(resData[8:], res.Body)
	_, err := conn.Write(resData)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}
}

