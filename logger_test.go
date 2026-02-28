package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestDefaultJSONLogger(t *testing.T) {
	logger := DefaultJSONLogger()
	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}
	// Check that it's a JSON handler
	handler := logger.Handler()
	if _, ok := handler.(*slog.JSONHandler); !ok {
		t.Errorf("Expected JSONHandler, got %T", handler)
	}
}

func TestDefaultTextLogger(t *testing.T) {
	logger := DefaultTextLogger()
	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}
	handler := logger.Handler()
	if _, ok := handler.(*slog.TextHandler); !ok {
		t.Errorf("Expected TextHandler, got %T", handler)
	}
}

func TestDefaultDebugJSONLogger(t *testing.T) {
	logger := DefaultDebugJSONLogger()
	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}
	// Verify debug level
	if logger.Enabled(context.Background(), slog.LevelDebug) != true {
		t.Error("Expected debug level to be enabled")
	}
}

func TestDefaultDebugTextLogger(t *testing.T) {
	logger := DefaultDebugTextLogger()
	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}
	if logger.Enabled(context.Background(), slog.LevelDebug) != true {
		t.Error("Expected debug level to be enabled")
	}
}

func TestNewJSONLoggerToFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_log_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	logger, err := NewJSONLoggerToFile(tmpFile.Name(), slog.LevelInfo)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("test message")
	tmpFile.Close()

	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "test message") {
		t.Error("Expected log message in file")
	}
}

func TestNewTextLoggerToFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_log_*.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	logger, err := NewTextLoggerToFile(tmpFile.Name(), slog.LevelInfo)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("test message")
	tmpFile.Close()

	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "test message") {
		t.Error("Expected log message in file")
	}
}

func TestNewLoggerToWriter_JSON(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerToWriter(&buf, true, slog.LevelInfo)
	logger.Info("test")
	if !strings.Contains(buf.String(), `"msg":"test"`) {
		t.Error("Expected JSON format")
	}
}

func TestNewLoggerToWriter_Text(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerToWriter(&buf, false, slog.LevelInfo)
	logger.Info("test")
	if !strings.Contains(buf.String(), "test") {
		t.Error("Expected text format")
	}
}

func TestMultiWriter_Write(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	mw := NewMultiWriter(&buf1, &buf2)
	data := []byte("hello")
	n, err := mw.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Errorf("Expected %d bytes written, got %d", len(data), n)
	}
	if buf1.String() != "hello" || buf2.String() != "hello" {
		t.Error("Expected data in both buffers")
	}
}

func TestNewMultiLogger(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	logger := NewMultiLogger(true, slog.LevelInfo, &buf1, &buf2)
	logger.Info("multi test")
	if !strings.Contains(buf1.String(), `"msg":"multi test"`) ||
		!strings.Contains(buf2.String(), `"msg":"multi test"`) {
		t.Error("Expected JSON in both buffers")
	}
}
