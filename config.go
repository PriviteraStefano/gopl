package pipeline

import (
	"context"
	"log/slog"
)

// Config holds configuration for pipeline observability
type Config struct {
	Observer Observer
	Logger   Logger
	Context  context.Context
}

// DefaultConfig returns a default pipeline configuration with no-op implementations
func DefaultConfig() *Config {
	return &Config{
		Observer: NoOpObserver{},
		Logger:   NewSlogLogger(slog.Default()),
		Context:  context.Background(),
	}
}

// DefaultConfigWithSlog creates a config with the provided slog.Logger
func DefaultConfigWithSlog(logger *slog.Logger) *Config {
	return &Config{
		Observer: NoOpObserver{},
		Logger:   NewSlogLogger(logger),
		Context:  context.Background(),
	}
}

// WithObserver sets the observer in the config
func (c *Config) WithObserver(observer Observer) *Config {
	c.Observer = observer
	return c
}

// WithLogger sets the logger in the config
func (c *Config) WithLogger(logger Logger) *Config {
	c.Logger = logger
	return c
}

// WithContext sets the context in the config
func (c *Config) WithContext(ctx context.Context) *Config {
	c.Context = ctx
	return c
}

func (config *Config) EmitEvent(event Eventful) {
	if config == nil || config.Observer == nil {
		return
	}

	config.Observer.OnEvent(config.Context, event)
}
