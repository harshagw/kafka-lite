package main

import (
	"fmt"
	"net"
	"os"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Connection accepted")

	for {
		reqData := make([]byte, 1024)
		n, err := conn.Read(reqData)
		if err != nil {
			fmt.Println("Error reading data: ", err.Error())
			return
		}
		if n == 0 {
			fmt.Println("Connection closed by client")
			return
		}
		reqData = reqData[:n]
		req, err := parseRequest(reqData)
		if err != nil {
			fmt.Println("Error parsing request: ", err.Error())
			return
		}

		var res *Response = nil
		switch req.RequestApiKey {
		case API_VERSIONS_REQUEST_KEY:
			res = handleApiVersionsRequest(req)
		case DESCRIBE_TOPIC_PARTITIONS_REQUEST_KEY:
			res = handleDescribeTopicPartitionsRequest(req)
		case FETCH_REQUEST_KEY:
			res = handleFetchRequest(req)
		default:
			fmt.Println("Unknown API key: ", req.RequestApiKey)
			return
		}

		result := encodeResponse(res)

		// Send the response
		_, err = conn.Write(result)
		if err != nil {
			fmt.Println("Error sending response: ", err.Error())
			os.Exit(1)
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	prepareLogFileData(LOGS_SRC_FOLDER)
	

	for {
		fmt.Println("Waiting for connection...")
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
