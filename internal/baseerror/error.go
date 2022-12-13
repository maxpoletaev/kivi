package baseerror

type Error struct {
	parent error
	msg    string
}

func New(msg string) *Error {
	return &Error{msg: msg}
}

func (err *Error) New(msg string) *Error {
	return &Error{
		parent: err,
		msg:    msg,
	}
}

func (err *Error) Error() string {
	return err.msg
}

func (err *Error) Unwrap() error {
	return err.parent
}
