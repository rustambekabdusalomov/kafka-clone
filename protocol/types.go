package protocol

const (
	MessageTypeHeartbeat       = 1
	MessageTypeRegisterBroker  = 2
	MessageTypeProduceRequest  = 3
	MessageTypeProductResponse = 4
)

type HeartBeat struct {
	BrokerID int32
}

type RegisterBroker struct {
	BrokerID int32
	Host     string
	Port     int
}
