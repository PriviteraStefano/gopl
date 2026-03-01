package pipeline

import (
	"context"
	"log/slog"
)

// Observer interface for observing pipeline events
type Observer interface {
	OnEvent(ctx context.Context, event Eventful)
}

// Logger interface wraps slog.Logger for pipeline observability
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	With(args ...any) Logger
}

// SlogLogger wraps slog.Logger to implement our Logger interface
type SlogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a Logger that wraps slog.Logger
func NewSlogLogger(logger *slog.Logger) Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogLogger{logger: logger}
}

func (s *SlogLogger) Debug(msg string, args ...any) {
	s.logger.Debug(msg, args...)
}

func (s *SlogLogger) Info(msg string, args ...any) {
	s.logger.Info(msg, args...)
}

func (s *SlogLogger) Warn(msg string, args ...any) {
	s.logger.Warn(msg, args...)
}

func (s *SlogLogger) Error(msg string, args ...any) {
	s.logger.Error(msg, args...)
}

func (s *SlogLogger) With(args ...any) Logger {
	return &SlogLogger{logger: s.logger.With(args...)}
}

// ObserverFunc is a function adapter for Observer interface
type ObserverFunc func(ctx context.Context, event Eventful)

func (f ObserverFunc) OnEvent(ctx context.Context, event Eventful) {
	f(ctx, event)
}

// MultiObserver combines multiple observers
type MultiObserver struct {
	observers []Observer
}

// NewMultiObserver creates a new MultiObserver
func NewMultiObserver(observers ...Observer) *MultiObserver {
	return &MultiObserver{observers: observers}
}

// OnEvent broadcasts the event to all observers
func (m *MultiObserver) OnEvent(ctx context.Context, event Eventful) {
	for _, observer := range m.observers {
		observer.OnEvent(ctx, event)
	}
}

// Add adds an observer to the MultiObserver
func (m *MultiObserver) Add(observer Observer) {
	m.observers = append(m.observers, observer)
}

// NoOpObserver is a no-operation observer (default)
type NoOpObserver struct{}

func (NoOpObserver) OnEvent(ctx context.Context, event Eventful) {}

// NoOpLogger is a no-operation logger (default)
type NoOpLogger struct{}

func (NoOpLogger) Debug(msg string, args ...any) {}
func (NoOpLogger) Info(msg string, args ...any)  {}
func (NoOpLogger) Warn(msg string, args ...any)  {}
func (NoOpLogger) Error(msg string, args ...any) {}
func (NoOpLogger) With(args ...any) Logger       { return NoOpLogger{} }

// LoggerObserver adapts a Logger to Observer interface
type LoggerObserver struct {
	logger Logger
}

// NewLoggerObserver creates a new LoggerObserver
func NewLoggerObserver(logger Logger) *LoggerObserver {
	return &LoggerObserver{logger: logger}
}

func createArgs(event Eventful, metadataLen int) (args []any) {
	// Each metadata entry needs 2 slots (key + value); the fixed fields
	// (ID, type, timestamp) need 6 more slots at the end.
	metadataSlots := metadataLen * 2
	args = make([]any, metadataSlots+6)
	args[metadataSlots] = IdKey
	args[metadataSlots+1] = event.GetID()
	args[metadataSlots+2] = TypeKey
	args[metadataSlots+3] = event.GetType()
	args[metadataSlots+4] = TimeKey
	args[metadataSlots+5] = event.GetTime()

	idx := 0
	for k, v := range event.GetMetadata() {
		args[idx] = k
		args[idx+1] = v
		idx += 2
	}
	return args
}

// OnEvent handles events by logging them with appropriate levels
func (l *LoggerObserver) OnEvent(ctx context.Context, event Eventful) {
	metadataLength := len(event.GetMetadata())
	args := createArgs(event, metadataLength)

	switch event := event.(type) {
	case *ErrorEvent:
		l.logger.Error(event.Error.Error(), args...)
	case *WarningEvent:
		l.logger.Warn(event.Warning, args...)
	case *DebugEvent:
		l.logger.Debug(event.Message, args...)
	case *Event:
		l.logger.Debug("", args...)
	default:
		l.logger.Info("", args...)
	}
}

const (
	IdKey   string = "ID"
	TypeKey string = "type"
	TimeKey string = "timestamp"
)
