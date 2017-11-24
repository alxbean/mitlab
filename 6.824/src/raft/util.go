package raft

import "log"
import "os"
import "fmt"

// Debugging
const Debug = 1
var logger *log.Logger

func LogInit(){
    file, _ := os.Create("lab2.log")

    logger = log.New(file, "", log.LstdFlags|log.Llongfile)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logger.Printf(format, a...)
	} else {
        fmt.Printf(format, a...)
    }
	return
}
