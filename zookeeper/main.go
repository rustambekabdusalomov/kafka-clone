package main

import (
	"log"
	"time"

	pkgzkpr "kafka-clone/pkg/zookeeper"
	"kafka-clone/protocol/server"
	"kafka-clone/zookeeper/handler"
	"kafka-clone/zookeeper/metadata"
)

type Zookeeper struct {
	Addr string
}

func NewZookeeper(addr string) *Zookeeper {
	return &Zookeeper{
		Addr: addr,
	}
}

func (z *Zookeeper) Start() error {
	srv := server.NewServer(z.Addr, 5*time.Second)
	db := metadata.NewMetadata()

	hdlr := handler.NewHandler(db)

	srv.RegisterHandler(pkgzkpr.RegisterBroker, hdlr.RegisterBroker)
	srv.RegisterHandler(pkgzkpr.HeartBeat, hdlr.Heartbeat)

	return srv.Listen()
}

func main() {
	zkpr := NewZookeeper("localhost:9091")

	log.Fatalln(zkpr.Start())
}
