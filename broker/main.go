package main

import (
	"flag"

	"kafka-clone/broker/broker"
)

func main() {
	port := flag.Int("port", 9092, "Port number for the server")
	host := flag.String("host", "localhost", "Host of broker")
	version := flag.Int("version", 1, "Broker Version")
	id := flag.Int("id", 1, "Broker ID")

	flag.Parse()

	brkr := broker.NewBroker(*port, *id, *version, *host)

	brkr.Start()
}
