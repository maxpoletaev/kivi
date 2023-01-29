package faildetector

import "time"

type Option func(*Detector)

func WithPingInterval(t time.Duration) Option {
	return func(d *Detector) {
		d.pingInterval = t
	}
}

func WithPingTimeout(t time.Duration) Option {
	return func(d *Detector) {
		d.pingTimeout = t
	}
}

func WithIndirectPingNodes(n int) Option {
	return func(d *Detector) {
		d.indirectPingNodes = n
	}
}
