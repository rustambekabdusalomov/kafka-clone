package broker

import (
	"fmt"
	"time"

	"kafka-clone/broker/handler"
	"kafka-clone/broker/zookeeper"
	zkprpkg "kafka-clone/pkg/zookeeper"
	"kafka-clone/protocol/server"
	zkrphandler "kafka-clone/zookeeper/handler"
)

type Broker struct {
	Port    int
	ID      int
	Version int
	Host    string
}

func NewBroker(port, id, version int, host string) *Broker {
	return &Broker{
		Port:    port,
		ID:      id,
		Version: version,
		Host:    host,
	}
}

func (b *Broker) GetAddr() string {
	if b.Port == 0 {
		return b.Host
	}

	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

func (b *Broker) Start() error {
	srv := server.NewServer(b.GetAddr(), 5*time.Second)
	h := handler.NewHandler()

	srv.RegisterHandler(zkprpkg.RegisterBroker, h.FetchMetaData)

	zkpr := zookeeper.NewZookeeper("localhost:9091")

	zkpr.RegisterBroker(zkrphandler.RegisterBrokerRequest{
		ID:      b.ID,
		Host:    b.Host,
		Port:    b.Port,
		Version: b.Version,
	})

	go func() {
		for {
			time.Sleep(5 * time.Second)

			zkpr.Heartbeat(zkrphandler.HeartBeatRequest{
				ID: b.ID,
			})
		}
	}()

	return srv.Listen()
}
