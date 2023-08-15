package logrus

import (
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/log"
	"github.com/sirupsen/logrus"
)

// Ensure log.Logger is fully implemented during compile time.
var _ log.Logger = (*adapter)(nil)

// adapter implements the `log.Logger` interface for logrus
type adapter struct {
	*logrus.Entry
}

// FromLogrus creates a new `log.Logger` from the provided entry
func FromLogrus(entry *logrus.Entry) log.Logger {
	return &adapter{entry}
}

// WithField adds a field to the log entry.
func (l *adapter) WithField(key string, val interface{}) log.Logger {
	return FromLogrus(l.Entry.WithField(key, val))
}

// WithFields adds multiple fields to a log entry.
func (l *adapter) WithFields(f log.Fields) log.Logger {
	return FromLogrus(l.Entry.WithFields(logrus.Fields(f)))
}

// WithError adds an error to the log entry
func (l *adapter) WithError(err error) log.Logger {
	return FromLogrus(l.Entry.WithError(err))
}
