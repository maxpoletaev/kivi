package membership

type Status int

const (
	StatusHealthy Status = iota + 1
	StatusFaulty
)

func (s Status) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusFaulty:
		return "faulty"
	default:
		return ""
	}
}
