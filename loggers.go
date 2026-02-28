package pipeline

import (
	"io"
	"log/slog"
	"os"
)

// DefaultJSONLogger creates a JSON slog logger writing to stdout
func DefaultJSONLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// DefaultTextLogger creates a text slog logger writing to stdout
func DefaultTextLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// DefaultDebugJSONLogger creates a debug-enabled JSON logger
func DefaultDebugJSONLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// DefaultDebugTextLogger creates a debug-enabled text logger
func DefaultDebugTextLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// NewJSONLoggerToFile creates a JSON slog logger that writes to a file
func NewJSONLoggerToFile(filename string, level slog.Level) (*slog.Logger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{
		Level: level,
	})), nil
}

// NewTextLoggerToFile creates a text slog logger that writes to a file
func NewTextLoggerToFile(filename string, level slog.Level) (*slog.Logger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return slog.New(slog.NewTextHandler(file, &slog.HandlerOptions{
		Level: level,
	})), nil
}

// NewLoggerToWriter creates an slog logger that writes to any io.Writer
func NewLoggerToWriter(writer io.Writer, useJSON bool, level slog.Level) *slog.Logger {
	var handler slog.Handler

	opts := &slog.HandlerOptions{Level: level}

	if useJSON {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	return slog.New(handler)
}

// MultiWriter combines multiple writers for logging to multiple destinations
type MultiWriter struct {
	writers []io.Writer
}

// NewMultiWriter creates a writer that writes to multiple destinations
func NewMultiWriter(writers ...io.Writer) *MultiWriter {
	return &MultiWriter{writers: writers}
}

// Write implements io.Writer interface
func (mw *MultiWriter) Write(p []byte) (n int, err error) {
	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return
		}
	}
	return len(p), nil
}

// NewMultiLogger creates an slog logger that writes to multiple destinations
func NewMultiLogger(useJSON bool, level slog.Level, writers ...io.Writer) *slog.Logger {
	multiWriter := NewMultiWriter(writers...)
	return NewLoggerToWriter(multiWriter, useJSON, level)
}
