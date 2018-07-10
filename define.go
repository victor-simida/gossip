package gossiper

type gossipRequest struct {
	Type int           `json:"type"`
	List []GossipValue `json:"list"`
}

// GossipValue ...
type GossipValue struct {
	Key        string      `json:"key"`
	Version    int64       `json:"version"`
	Value      interface{} `json:"value"`
	DeleteFlag bool        `json:"flag"`
}
