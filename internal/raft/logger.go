package raft

import (
	"log"
	"os"
)

var (
	LogLevel = 1 // 0=ERROR, 1=WARN, 2=INFO, 3=DEBUG
)

func init() {
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		switch level {
		case "ERROR":
			LogLevel = 0
		case "WARN":
			LogLevel = 1
		case "INFO":
			LogLevel = 2
		case "DEBUG":
			LogLevel = 3
		}
	}
}

func logDebug(format string, v ...interface{}) {
	if LogLevel >= 3 {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func logInfo(format string, v ...interface{}) {
	if LogLevel >= 2 {
		log.Printf("[INFO] "+format, v...)
	}
}

func logWarn(format string, v ...interface{}) {
	if LogLevel >= 1 {
		log.Printf("[WARN] "+format, v...)
	}
}

func logError(format string, v ...interface{}) {
	if LogLevel >= 0 {
		log.Printf("[ERROR] "+format, v...)
	}
}
