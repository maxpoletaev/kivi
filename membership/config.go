package membership

import (
	"time"

	kitlog "github.com/go-kit/log"

	"github.com/maxpoletaev/kiwi/nodeapi"
)

type Config struct {
	Dialer        nodeapi.Dialer
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
