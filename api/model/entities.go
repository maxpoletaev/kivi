package model

type Node struct {
	ID     uint32 `json:"ID"`
	Name   string `json:"Name"`
	Addr   string `json:"Addr"`
	Status string `json:"Status"`
	Error  string `json:",omitempty"`
}
