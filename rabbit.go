package rabbit

import (
	"fmt"
)

var (
	logInfo = func(i ...interface{}) {
		j := []interface{}{"Info:"}
		j = append(j, i...)
		fmt.Println(j...)
	}
	logError = func(i ...interface{}) {
		j := []interface{}{"Error:"}
		j = append(j, i...)
		fmt.Println(j...)
	}
	logWarning = func(i ...interface{}) {
		j := []interface{}{"Warning:"}
		j = append(j, i...)
		fmt.Println(j...)
	}
)

func SetLogInfo(f func(i ...interface{})) {
	logInfo = f
}

func SetLogError(f func(i ...interface{})) {
	logError = f
}

func SetLogWarning(f func(i ...interface{})) {
	logWarning = f
}
