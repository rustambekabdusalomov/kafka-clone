package handler

import (
	"context"
	"log"
	"time"

	"kafka-clone/protocol/client"
	"kafka-clone/zookeeper/broker"
	"kafka-clone/zookeeper/metadata"
)

type Handler struct {
	metadata *metadata.Metadata
}

func NewHandler(metadata *metadata.Metadata) *Handler {
	return &Handler{
		metadata: metadata,
	}
}

func (h *Handler) RegisterBroker(ctx context.Context, req *client.Request) (*client.Response, error) {
	reqBody := &RegisterBrokerRequest{}
	req.ParseBody(reqBody)
	h.metadata.AddBroker(&broker.Broker{
		ID:        reqBody.ID,
		Port:      reqBody.Port,
		Version:   reqBody.Version,
		Host:      reqBody.Host,
		Timestamp: time.Now().Unix(),
	})

	log.Println("metadata: ", h.metadata)

	return client.NewResponse(req.Header.RequestID, 200, []byte("ok!")), nil
}

func (h *Handler) Heartbeat(ctx context.Context, req *client.Request) (*client.Response, error) {
	reqBody := &HeartBeatRequest{}
	req.ParseBody(reqBody)
	h.metadata.BrokerHeartbeat(reqBody.ID)

	log.Println("Hearbeat: ", reqBody.ID)

	return client.NewResponse(req.Header.RequestID, 200, []byte("ok!")), nil
}
