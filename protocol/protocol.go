package protocol

import (
	"bytes"
	"encoding/binary"
)

const (
	MessageTypeMetadataRequest        = 1
	MessageTypeMetadataResponse       = 2
	MessageTypeBrokerRegisterRequest  = 3
	MessageTypeBrokerRegisterResponse = 4
)

type Packet struct {
	MessageType uint16
	Payload     []byte
}

func EncodePacket(messageType uint16, payload []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	totalLength := uint32(2 + len(payload))
	if err := binary.Write(buf, binary.BigEndian, totalLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, messageType); err != nil {
		return nil, err
	}
	if _, err := buf.Write(payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodePacket(data []byte) (*Packet, error) {
	buf := bytes.NewReader(data)

	var length uint32
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	var messageType uint16
	if err := binary.Read(buf, binary.BigEndian, &messageType); err != nil {
		return nil, err
	}

	payload := make([]byte, length-2)
	if _, err := buf.Read(payload); err != nil {
		return nil, err
	}

	return &Packet{
		MessageType: messageType,
		Payload:     payload,
	}, nil
}

func EncodeMetadataRequest(topic string) ([]byte, error) {
	buf := new(bytes.Buffer)
	topicBytes := []byte(topic)
	topicLen := uint16(len(topicBytes))
	if err := binary.Write(buf, binary.BigEndian, topicLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(topicBytes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeMetadataRequest(data []byte) (string, error) {
	buf := bytes.NewReader(data)
	var topicLen uint16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return "", err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := buf.Read(topicBytes); err != nil {
		return "", err
	}
	return string(topicBytes), nil
}
