package zookeeper

import (
	"encoding/json"
	"log"
	"time"

	zkprpkg "kafka-clone/pkg/zookeeper"
	"kafka-clone/protocol/client"
	"kafka-clone/zookeeper/handler"
)

type Zookeeper struct {
	addr string
	tcp  *client.Client
}

func NewZookeeper(addr string) *Zookeeper {
	cln := client.NewClient("localhost:9091", time.Second*5)

	go func() {
		cln.Listen()
	}()

	return &Zookeeper{
		addr: addr,
		tcp:  cln,
	}
}

func (z *Zookeeper) RegisterBroker(req handler.RegisterBrokerRequest) {
	reqByte, err := json.Marshal(req)
	if err != nil {
		log.Fatalln(err)
	}
	_, err = z.tcp.SendRequest(zkprpkg.RegisterBroker, client.NewRequest(reqByte))
	if err != nil {
		log.Fatalln(err)
	}
}

func (z *Zookeeper) Heartbeat(req handler.HeartBeatRequest) {
	reqByte, err := json.Marshal(req)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = z.tcp.SendRequest(zkprpkg.HeartBeat, client.NewRequest(reqByte))
	if err != nil {
		log.Fatalln(err)
	}
}
