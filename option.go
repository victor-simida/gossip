package gossiper

import "time"

// Option ...
type Option struct {
	SyncPeriod time.Duration
	PeerList   []string
	ListenPort int
}

var DefaultOption = Option{
	SyncPeriod: 5,
	ListenPort: 5100,
}

type ServerOption func(option *Option)

// WithSyncPeriod set option with sync period
func WithSyncPeriod(input time.Duration) ServerOption {
	return func(option *Option) {
		option.SyncPeriod = input
	}
}

// WithListenPort set option with listen port
func WithListenPort(input int) ServerOption {
	return func(option *Option) {
		option.ListenPort = input
	}
}

// WithListenPort set option with peer list
func WithPeerList(input []string) ServerOption {
	return func(option *Option) {
		option.PeerList = input
	}
}
