package main

import (
	"context"
	"log"
	"time"

	"kafka-clone/protocol/client"
	"kafka-clone/protocol/server"
)

func main() {
	srv := server.NewServer("localhost:9092", time.Second*5)

	srv.RegisterHandler(1, func(ctx context.Context, r *client.Request) (*client.Response, error) {
		body := struct {
			Name string
		}{}

		r.ParseBody(&body)

		log.Println("BODY: ", body)

		return client.NewResponse(r.Header.RequestID, 200, nil), nil
	})

	srv.Listen()
}
