package handler

import (
	"context"

	"kafka-clone/protocol/client"
)

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) FetchMetaData(ctx context.Context, req *client.Request) (*client.Response, error) {
	return client.NewResponse(req.Header.RequestID, 200, []byte("")), nil
}
