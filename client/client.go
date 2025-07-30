package main

import (
	"fmt"
	"log"
	"time"

	"kafka-clone/protocol/client"
)

func main() {
	cln := client.NewClient("localhost:9092", time.Second*5)

	go func() {
		cln.Listen()
	}()

	time.Sleep(1 * time.Second)

	for i := range 100000 {
		go func(idx int) {
			res, _ := cln.SendRequest(1, client.NewRequest([]byte(fmt.Sprintf(`{"name": "User%d"}`, idx))))
			log.Println("res:", idx, res.Body)
		}(i)
	}

	time.Sleep(3 * time.Second)
}
