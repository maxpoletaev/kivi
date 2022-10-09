package clustering

type Status int

const (
	StatusAlive Status = iota + 1
	StatusDead
)

func (s Status) String() string {
	switch s {
	case StatusAlive:
		return "alive"
	case StatusDead:
		return "dead"
	default:
		return ""
	}
}
