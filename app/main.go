package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)


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

	resData := make([]byte, 8)
	
	messageSize := int32(0)
	binary.BigEndian.PutUint32(resData[0:4], uint32(messageSize))
	
	correlationId := int32(7)
	binary.BigEndian.PutUint32(resData[4:8], uint32(correlationId))
	
	_, err = conn.Write(resData)
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
}
