package queuetest

import (
	"context"
)

// LogEntry encapsulates logged fields.
type LogEntry struct {
	Msg           string
	Err           error
	KeysAndValues []interface{}
}

// FakeLogger implements queue.Logger for testing purposes.
type FakeLogger struct {
	InfoFunc  func(ctx context.Context, msg string, keysAndValues ...interface{})
	ErrorFunc func(ctx context.Context, msg string, err error, keysAndValues ...interface{})
	Infos     []LogEntry
	Errors    []LogEntry
}

func (l *FakeLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	entry := LogEntry{Msg: msg, KeysAndValues: keysAndValues}
	l.Infos = append(l.Infos, entry)
	if l.InfoFunc != nil {
		l.InfoFunc(ctx, msg, keysAndValues...)
	}
}

func (l *FakeLogger) Error(ctx context.Context, msg string, err error, keysAndValues ...interface{}) {
	entry := LogEntry{Msg: msg, Err: err, KeysAndValues: keysAndValues}
	l.Errors = append(l.Errors, entry)
	if l.ErrorFunc != nil {
		l.ErrorFunc(ctx, msg, err, keysAndValues...)
	}
}
