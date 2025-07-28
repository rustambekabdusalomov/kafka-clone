package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type Response struct {
	Header *ResponseHeader
	Body   *ResponseBody
}

func NewResponse(requestID string, messageType uint16, statusCode uint16, data []byte) *Response {
	if data == nil {
		data = []byte{}
	}

	return &Response{
		Header: &ResponseHeader{
			Version:     1,
			RequestID:   requestID,
			MessageType: messageType,
			Length:      uint32(len(data)),
			CreatedAt:   time.Now().Unix(),
			StatusCode:  statusCode,
		},
		Body: &ResponseBody{
			Data: data,
		},
	}
}

type ResponseHeader struct {
	Version     uint8
	RequestID   string
	CreatedAt   int64
	StatusCode  uint16
	MessageType uint16
	Length      uint32
	ClientID    uint16
}

type ResponseBody struct {
	Data []byte
}

func (res *Response) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write Version
	if err := binary.Write(&buf, binary.BigEndian, res.Header.Version); err != nil {
		return nil, err
	}

	// Write ClientID
	if err := binary.Write(&buf, binary.BigEndian, res.Header.ClientID); err != nil {
		return nil, err
	}

	// Write RequestID length and value
	requestIDBytes := []byte(res.Header.RequestID)
	requestIDLen := uint8(len(requestIDBytes))
	if err := binary.Write(&buf, binary.BigEndian, requestIDLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(requestIDBytes); err != nil {
		return nil, err
	}

	// Write CreatedAt
	if err := binary.Write(&buf, binary.BigEndian, res.Header.CreatedAt); err != nil {
		return nil, err
	}

	// Write StatusCode
	if err := binary.Write(&buf, binary.BigEndian, res.Header.StatusCode); err != nil {
		return nil, err
	}

	// Write MessageType
	if err := binary.Write(&buf, binary.BigEndian, res.Header.MessageType); err != nil {
		return nil, err
	}

	// Write Length
	if err := binary.Write(&buf, binary.BigEndian, res.Header.Length); err != nil {
		return nil, err
	}

	// Write Body
	if _, err := buf.Write(res.Body.Data); err != nil {
		return nil, err
	}

	resBuf := new(bytes.Buffer)
	bufLength := uint32(len(buf.Bytes()))

	if err := binary.Write(resBuf, binary.BigEndian, bufLength); err != nil {
		return nil, fmt.Errorf("writing Buffer Length: %w", err)
	}

	if _, err := resBuf.Write(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("writing Buffer: %w", err)
	}

	return resBuf.Bytes(), nil
}

func DecodeResponse(data []byte) (*Response, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}

	buf := bytes.NewReader(data)

	var version uint8
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var clientID uint16
	if err := binary.Read(buf, binary.BigEndian, &clientID); err != nil {
		return nil, err
	}

	// Read RequestID
	var requestIDLen uint8
	if err := binary.Read(buf, binary.BigEndian, &requestIDLen); err != nil {
		return nil, err
	}
	requestIDBytes := make([]byte, requestIDLen)
	if _, err := buf.Read(requestIDBytes); err != nil {
		return nil, err
	}
	requestID := string(requestIDBytes)

	// Read CreatedAt
	var createdAt int64
	if err := binary.Read(buf, binary.BigEndian, &createdAt); err != nil {
		return nil, err
	}

	// Read StatusCode
	var statusCode uint16
	if err := binary.Read(buf, binary.BigEndian, &statusCode); err != nil {
		return nil, err
	}

	// Read MessageType
	var messageType uint16
	if err := binary.Read(buf, binary.BigEndian, &messageType); err != nil {
		return nil, err
	}

	// Read Length
	var length uint32
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	res := &Response{
		Header: &ResponseHeader{
			Version:     version,
			RequestID:   requestID,
			CreatedAt:   createdAt,
			StatusCode:  statusCode,
			MessageType: messageType,
			Length:      length,
		},
		Body: &ResponseBody{},
	}

	if length > 0 {
		// Read Body
		body := make([]byte, length)
		if _, err := buf.Read(body); err != nil {
			return nil, err
		}
		res.Body.Data = body
	} else {
		res.Body.Data = nil
	}

	return res, nil
}
