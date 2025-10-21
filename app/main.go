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
		req := parseRequest(reqData)

		res := Response{}
		switch req.RequestApiKey {
		case 18: // ApiVersions
			res = handleApiVersionsRequest(req)
		default:
			fmt.Println("Unknown API key: ", req.RequestApiKey)
			return
		}

		sendResponse(conn, res)
	}
}


func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

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
