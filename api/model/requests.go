package model

type PutKeyParams struct {
	Value   string `json:"Value"`
	Version string `json:"Version"`
}

type PutKeyResponse struct {
	Version      string `json:"Version"`
	Acknowledged int    `json:"Acknowledged"`
}

type DeleteKeyParams struct {
	Version string
}

type DeleteKeyResponse struct {
	Version      string `json:"Version"`
	Acknowledged int    `json:"Acknowledged"`
}

type GetKeyResponse struct {
	Values  []string `json:"Values,omitempty"`
	Value   string   `json:"Value,omitempty"`
	Version string   `json:"Version"`
	Exists  bool     `json:"Exists"`
}

type GetNodesResponse struct {
	Nodes []Node `json:"Nodes"`
}
