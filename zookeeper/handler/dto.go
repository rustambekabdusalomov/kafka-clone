package handler

type RegisterBrokerRequest struct {
	ID      int    `json:"id"`
	Port    int    `json:"port"`
	Version int    `json:"version"`
	Host    string `json:"host"`
}

type RegisterBrokerResponse struct {
	ID int
}

type HeartBeatRequest struct {
	ID int
}
