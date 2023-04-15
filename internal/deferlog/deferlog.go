package deferlog

import (
	"fmt"
	"log"
	"os"
)

var logger *log.Logger

func init() {
	logger = log.New(os.Stderr, "deferlog: ", log.Lshortfile)
}

func Warn(format string, v ...interface{}) {
	logger.Output(2, fmt.Sprintf(format, v...)) //nolint:errcheck
}
