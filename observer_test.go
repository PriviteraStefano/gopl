package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger := NewSlogLogger(slogLogger)

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	output := buf.String()
	if !strings.Contains(output, "debug msg") ||
		!strings.Contains(output, "info msg") ||
		!strings.Contains(output, "warn msg") ||
		!strings.Contains(output, "error msg") {
		t.Error("Expected all log levels in output")
	}
}

func TestSlogLogger_With(t *testing.T) {
	var buf bytes.Buffer
	slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger := NewSlogLogger(slogLogger).With("key", "value")
	logger.Info("test")
	if !strings.Contains(buf.String(), "key=value") {
		t.Error("Expected context in log")
	}
}

func TestNoOpLogger(t *testing.T) {
	logger := NoOpLogger{}
	// These should not panic or do anything
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
	withLogger := logger.With("key", "value")
	if withLogger == nil {
		t.Error("Expected non-nil logger from With")
	}
}

func TestMultiObserver_OnEvent(t *testing.T) {
	called := make([]bool, 2)
	obs1 := ObserverFunc(func(ctx context.Context, event Eventful) { called[0] = true })
	obs2 := ObserverFunc(func(ctx context.Context, event Eventful) { called[1] = true })

	multi := NewMultiObserver(obs1, obs2)
	event := &Event{ID: "test", Type: ItemProcessed, Time: time.Now()}
	multi.OnEvent(context.Background(), event)

	if !called[0] || !called[1] {
		t.Error("Expected both observers to be called")
	}
}

func TestMultiObserver_Add(t *testing.T) {
	multi := NewMultiObserver()
	called := false
	obs := ObserverFunc(func(ctx context.Context, event Eventful) { called = true })
	multi.Add(obs)

	event := &Event{ID: "test", Type: ItemProcessed, Time: time.Now()}
	multi.OnEvent(context.Background(), event)

	if !called {
		t.Error("Expected added observer to be called")
	}
}

func TestNoOpObserver_OnEvent(t *testing.T) {
	obs := NoOpObserver{}
	// Should not panic
	obs.OnEvent(context.Background(), &Event{ID: "test", Type: ItemProcessed, Time: time.Now()})
}

func TestLoggerObserver_OnEvent(t *testing.T) {
	var buf bytes.Buffer
	slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger := NewSlogLogger(slogLogger)
	obs := NewLoggerObserver(logger)

	// Test different event types
	events := []Eventful{
		&Event{ID: "1", Type: ItemProcessed, Time: time.Now(), Metadata: map[string]any{"key": "value"}},
		&DebugEvent{Event: Event{ID: "2", Type: DebugEventType, Time: time.Now()}, Message: "debug"},
		&WarningEvent{Event: Event{ID: "3", Type: WarningEventType, Time: time.Now()}, Warning: "warn"},
		&ErrorEvent{Event: Event{ID: "4", Type: ErrorEventType, Time: time.Now()}, Error: &testError{msg: "err"}},
	}

	for _, event := range events {
		obs.OnEvent(context.Background(), event)
	}

	output := buf.String()
	if !strings.Contains(output, "ID=1") ||
		!strings.Contains(output, "debug") ||
		!strings.Contains(output, "warn") ||
		!strings.Contains(output, "err") {
		t.Error("Expected event details in log")
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
