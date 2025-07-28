package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"kafka-clone/protocol/types"
)

type Request struct {
	Header *RequestHeader
	Body   *RequestBody
}

func NewRequest(messageType uint16, data []byte) *Request {
	if data == nil {
		data = []byte{}
	}

	req := &Request{
		Header: &RequestHeader{
			Version:     1,
			RequestID:   uuid.NewString(),
			CreatedAt:   time.Now().Unix(),
			MessageType: messageType,
			Length:      uint32(len(data)),
		},
		Body: &RequestBody{
			Data: data,
		},
	}

	req.Header.RequestIDLength = uint8(len(req.Header.RequestID))

	return req
}

type RequestHeader struct {
	Version         uint8
	RequestID       string
	RequestIDLength uint8
	CreatedAt       int64
	MessageType     uint16
	Length          uint32
	ClientID        uint16
	ReqType         types.ReqType
}

type RequestBody struct {
	Data []byte
}

func (r *Request) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write Header fields
	if err := binary.Write(buf, binary.BigEndian, r.Header.Version); err != nil {
		return nil, fmt.Errorf("write Version: %w", err)
	}

	// Write Header fields
	if err := binary.Write(buf, binary.BigEndian, r.Header.ReqType); err != nil {
		return nil, fmt.Errorf("write Version: %w", err)
	}

	// Write ClientID fields
	if err := binary.Write(buf, binary.BigEndian, r.Header.ClientID); err != nil {
		return nil, fmt.Errorf("write ClientID: %w", err)
	}

	// Write RequestID Length + RequestID
	if err := binary.Write(buf, binary.BigEndian, r.Header.RequestIDLength); err != nil {
		return nil, fmt.Errorf("write RequestID length: %w", err)
	}
	if _, err := buf.Write([]byte(r.Header.RequestID)); err != nil {
		return nil, fmt.Errorf("write requestID: %w", err)
	}

	// Write CreatedAt
	if err := binary.Write(buf, binary.BigEndian, r.Header.CreatedAt); err != nil {
		return nil, fmt.Errorf("writing CreatedAt: %w", err)
	}

	// Write MessageType
	if err := binary.Write(buf, binary.BigEndian, r.Header.MessageType); err != nil {
		return nil, fmt.Errorf("writing MessageType: %w", err)
	}

	// Write Length
	if err := binary.Write(buf, binary.BigEndian, r.Header.Length); err != nil {
		return nil, fmt.Errorf("writing Length: %w", err)
	}

	// Write Data
	if _, err := buf.Write(r.Body.Data); err != nil {
		return nil, fmt.Errorf("writing Body: %w", err)
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

func DecodeRequest(data []byte) (*Request, error) {
	if data == nil {
		return nil, fmt.Errorf("data is nil")
	}

	buf := bytes.NewReader(data)

	var version uint8
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("reading Version: %w", err)
	}

	var reqType uint8
	if err := binary.Read(buf, binary.BigEndian, &reqType); err != nil {
		return nil, fmt.Errorf("reading ReqType: %w", err)
	}

	var clietnID uint16
	if err := binary.Read(buf, binary.BigEndian, &clietnID); err != nil {
		return nil, fmt.Errorf("reading ClientID: %w", err)
	}

	// Read RequestID length
	var requestIDLen uint8
	if err := binary.Read(buf, binary.BigEndian, &requestIDLen); err != nil {
		return nil, fmt.Errorf("reading RequestIDLength: %w", err)
	}

	// Read RequestID
	requestIDBytes := make([]byte, requestIDLen)
	if _, err := io.ReadFull(buf, requestIDBytes); err != nil {
		return nil, fmt.Errorf("reading RequestID: %w", err)
	}
	requestID := string(requestIDBytes)

	// Read CreatedAt
	var createdAt int64
	if err := binary.Read(buf, binary.BigEndian, &createdAt); err != nil {
		return nil, fmt.Errorf("reading CreatedAt: %w", err)
	}

	// Read MessageType
	var messageType uint16
	if err := binary.Read(buf, binary.BigEndian, &messageType); err != nil {
		return nil, fmt.Errorf("reading MessageType: %w", err)
	}

	// Read Length
	var length uint32
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("reading Length: %w", err)
	}

	req := &Request{
		Header: &RequestHeader{
			Version:         version,
			RequestID:       requestID,
			RequestIDLength: requestIDLen,
			CreatedAt:       createdAt,
			MessageType:     messageType,
			Length:          length,
			ClientID:        clietnID,
		},
		Body: &RequestBody{},
	}

	if length > 0 {
		// Read Data
		body := make([]byte, length)
		if _, err := io.ReadFull(buf, body); err != nil {
			return nil, fmt.Errorf("reading Body: %w", err)
		}
		req.Body.Data = body
	} else {
		req.Body.Data = nil
	}

	return req, nil
}
