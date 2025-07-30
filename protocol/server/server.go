package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"kafka-clone/protocol/client"
	"kafka-clone/protocol/types"
)

type Server struct {
	Addr     string
	Timeout  time.Duration
	Requests map[int32]map[string]*client.Request
	Handlers map[types.ReqType]Handler
}

func NewServer(addr string, timeout time.Duration) *Server {
	return &Server{
		Addr:     addr,
		Timeout:  timeout,
		Handlers: make(map[types.ReqType]Handler),
	}
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	fmt.Printf("server listening on %s\n", s.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("error accepting connection: %v\n", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		header := make([]byte, 4)
		_, err := io.ReadFull(conn, header)
		if err == io.EOF {
			fmt.Println("connection closed")
			return
		}
		if err != nil {
			fmt.Println("Failed to read header:", err)
			log.Println("Requests", s.Requests)
			return
		}
		msgLen := binary.BigEndian.Uint32(header)

		msg := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msg); err != nil {
			fmt.Println("Failed to read message:", err)
			return
		}

		req, err := client.DecodeRequest(msg)
		if err != nil {
			fmt.Println("Failed to decode msg:", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		defer cancel()

		if _, ok := s.Handlers[req.Header.ReqType]; !ok {
			fmt.Println("ReqType not found:", err)
			continue
		}

		res, err := s.Handlers[req.Header.ReqType](ctx, req)
		if err != nil {
			fmt.Println("Handler error:", err)
			continue
		}

		resByte, err := res.Encode()
		if err != nil {
			fmt.Println("Encode response error:", err)
			continue
		}

		_, err = conn.Write(resByte)
		if err != nil {
			fmt.Println("Write response error:", err)
			continue
		}
	}

}
