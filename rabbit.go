package rabbit

import (
	"fmt"
)

var (
	logInfo = func(i ...interface{}) {
		fmt.Println(i)
	}
	logError = func(i ...interface{}) {
		fmt.Println(i)
	}
)

func SetLogInfo(f func(i ...interface{})) {
	logInfo = f
}

func SetLogError(f func(i ...interface{})) {
	logError = f
}
