package redis

// Logger is the interface that provides methods to log messages.
type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

// noopLogger is a logger that discards all messages.
type noopLogger struct{}

func (d noopLogger) Debugf(format string, v ...interface{}) {}

func (d noopLogger) Infof(format string, v ...interface{}) {}

func (d noopLogger) Warnf(format string, v ...interface{}) {}

func (d noopLogger) Errorf(format string, v ...interface{}) {}
