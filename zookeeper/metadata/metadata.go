package metadata

import (
	"time"

	"kafka-clone/zookeeper/broker"
)

type Metadata struct {
	Brokers map[int]*broker.Broker
}

func NewMetadata() *Metadata {
	return &Metadata{
		Brokers: map[int]*broker.Broker{},
	}
}

func (m *Metadata) AddBroker(brk *broker.Broker) {
	m.Brokers[brk.ID] = brk
}

func (m *Metadata) DelBroker(id int) {
	delete(m.Brokers, id)
}

func (m *Metadata) BrokerHeartbeat(id int) {
	if _, ok := m.Brokers[id]; !ok {
		return
	}
	m.Brokers[id].Timestamp = time.Now().Unix()
}
