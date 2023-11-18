package membership

import (
	"time"

	kitlog "github.com/go-kit/log"

	"github.com/maxpoletaev/kivi/noderpc"
)

type Config struct {
	NodeID        NodeID
	NodeName      string
	PublicAddr    string
	LocalAddr     string
	Dialer        noderpc.Dialer
	Logger        kitlog.Logger
	DialTimeout   time.Duration
	ProbeTimeout  time.Duration
	ProbeInterval time.Duration
	ProbeJitter   float64
	GCInterval    time.Duration
	IndirectNodes int
}

func DefaultConfig() Config {
	return Config{
		Logger:        kitlog.NewNopLogger(),
		DialTimeout:   6 * time.Second,
		ProbeTimeout:  3 * time.Second,
		ProbeInterval: 1 * time.Second,
		GCInterval:    30 * time.Second,
		IndirectNodes: 1,
	}
}
