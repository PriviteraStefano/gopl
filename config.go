package gopl

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

// Clone returns a shallow copy of the Config.  Use this before mutating a
// Config that may already be in use by running goroutines.
func (c *Config) Clone() *Config {
	if c == nil {
		return DefaultConfig()
	}
	clone := *c
	return &clone
}

// WithObserver returns a new Config with the observer replaced.
func (c *Config) WithObserver(observer Observer) *Config {
	clone := c.Clone()
	clone.Observer = observer
	return clone
}

// WithLogger returns a new Config with the logger replaced.
func (c *Config) WithLogger(logger Logger) *Config {
	clone := c.Clone()
	clone.Logger = logger
	return clone
}

// WithContext returns a new Config with the context replaced.
// It always allocates a fresh Config so that it is safe to call on a Config
// that is already referenced by running goroutines.
func (c *Config) WithContext(ctx context.Context) *Config {
	clone := c.Clone()
	clone.Context = ctx
	return clone
}

// ctx returns the config's context, falling back to context.Background() when
// the Context field has not been set (e.g. a Config created via a struct
// literal without the Context field).
func (config *Config) ctx() context.Context {
	if config == nil || config.Context == nil {
		return context.Background()
	}
	return config.Context
}

func (config *Config) EmitEvent(event Eventful) {
	if config == nil || config.Observer == nil {
		return
	}

	config.Observer.OnEvent(config.ctx(), event)
}
