package mr

import "fmt"

var debugOption bool = true

func MyPrintf(format string, a ...interface{}) {
	prefix := "[For 6.824 Debug] "
	if debugOption {
		fmt.Printf(prefix+format, a...)
	}
}
