package membership

import (
	"time"

	kitlog "github.com/go-kit/log"

	"github.com/maxpoletaev/kivi/nodeclient"
)

type Config struct {
	NodeID        NodeID
	NodeName      string
	PublicAddr    string
	LocalAddr     string
	Dialer        nodeclient.Dialer
	Logger        kitlog.Logger
	DialTimeout   time.Duration
	ProbeTimeout  time.Duration
	ProbeInterval time.Duration
	GCInterval    time.Duration
}

func DefaultConfig() Config {
	return Config{
		Logger:        kitlog.NewNopLogger(),
		DialTimeout:   6 * time.Second,
		ProbeTimeout:  2 * time.Second,
		ProbeInterval: 1 * time.Second,
		GCInterval:    30 * time.Second,
	}
}
