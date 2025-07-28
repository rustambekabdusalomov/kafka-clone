package server

import (
	"context"

	"kafka-clone/protocol/client"
	"kafka-clone/protocol/types"
)

type Handler func(context.Context, *client.Request) (*client.Response, error)

func (s *Server) RegisterHandler(reqType types.ReqType, handler Handler) {
	if s.Handlers == nil {
		s.Handlers = make(map[types.ReqType]Handler)
	}

	if _, ok := s.Handlers[reqType]; ok {
		panic("handler is already registered")
	}

	s.Handlers[reqType] = handler
}
