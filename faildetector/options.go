package faildetector

import "time"

type option func(*Detector)

func WithPingInterval(t time.Duration) option {
	return func(d *Detector) {
		d.pingInterval = t
	}
}

func WithPingTimeout(t time.Duration) option {
	return func(d *Detector) {
		d.pingTimeout = t
	}
}

func WithIndirectPingNodes(n int) option {
	return func(d *Detector) {
		d.indirectPingNodes = n
	}
}
