package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"kafka-clone/protocol/types"
)

type Client struct {
	Addr       string
	Timeout    time.Duration
	ReqQueue   sync.Map
	ClientID   uint16
	ReqChannel chan *Request
}

func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		Addr:       addr,
		Timeout:    timeout,
		ReqQueue:   sync.Map{},
		ReqChannel: make(chan *Request, 100),
		ClientID:   1,
	}
}

func (c *Client) Listen() error {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Start goroutine to read from server
	go func() {
		for {
			header := make([]byte, 4)
			_, err := io.ReadFull(conn, header)
			if err != nil {
				fmt.Println("Faild to read header:", err)
				return
			}
			msgLen := binary.BigEndian.Uint32(header)

			msg := make([]byte, msgLen)
			_, err = io.ReadFull(conn, msg)
			if err != nil {
				fmt.Println("Faild to read message:", err)
				return
			}

			resp, err := DecodeResponse(msg)
			if err != nil {
				fmt.Println("Faild to decode msg: ", err)
				return
			}

			val, ok := c.ReqQueue.Load(resp.Header.RequestID)
			if !ok {
				fmt.Println("Request ID not found: ", resp.Header.RequestID)
			}
			ch := val.(chan *Response)
			ch <- resp
		}
	}()

	for {
		req := <-c.ReqChannel
		reqByte, err := req.Encode()
		if err != nil {
			fmt.Println("Encode error: ", err)
			continue
		}

		_, err = conn.Write(reqByte)
		if err != nil {
			fmt.Println("Write error: ", err)
			continue
		}
	}
}

func (c *Client) SendRequest(reqType types.ReqType, req *Request) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	req.Header.ClientID = uint16(c.ClientID)
	req.Header.ReqType = reqType

	respChan := make(chan *Response)
	c.ReqQueue.Store(req.Header.RequestID, respChan)
	defer close(respChan)
	defer c.ReqQueue.Delete(req.Header.RequestID)

	c.ReqChannel <- req

	select {
	case res := <-respChan:
		return res, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("request timeout: %w", ctx.Err())
	}
}
