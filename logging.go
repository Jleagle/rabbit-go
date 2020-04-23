package rabbit

import (
	"fmt"
)

var (
	debug   bool
	logInfo = func(i ...interface{}) {
		if debug {
			j := []interface{}{"Info:"}
			j = append(j, i...)
			fmt.Println(j...)
		}
	}
	logWarning = func(i ...interface{}) {
		if debug {
			j := []interface{}{"Warning:"}
			j = append(j, i...)
			fmt.Println(j...)
		}
	}
	logError = func(i ...interface{}) {
		j := []interface{}{"Error:"}
		j = append(j, i...)
		fmt.Println(j...)
	}
)

//noinspection GoUnusedExportedFunction
func SetLogInfo(f func(i ...interface{})) {
	logInfo = f
}

//noinspection GoUnusedExportedFunction
func SetLogWarning(f func(i ...interface{})) {
	logWarning = f
}

//noinspection GoUnusedExportedFunction
func SetLogError(f func(i ...interface{})) {
	logError = f
}

func SetDebug(enable bool) {
	debug = enable
}
