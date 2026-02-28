# pipeline

A Go library for building concurrent data-processing pipelines using channels and goroutines. It provides composable building blocks for stages, routing, fan-in, observability, and result handling — all backed by Go generics.

Requires **Go 1.24+**.

---

## Table of Contents

- [Core Concepts](#core-concepts)
- [Configuration](#configuration)
- [Stages](#stages)
- [Routing](#routing)
- [Utilities](#utilities)
- [Result Type](#result-type)
- [Observability](#observability)
  - [Events](#events)
  - [Observer](#observer)
  - [Metrics](#metrics)
  - [Logging](#logging)
- [Type Reference](#type-reference)

---

## Core Concepts

### Identifier

Every item that flows through a `StartStage` pipeline must satisfy the `Identifier` interface:

```go
type Identifier interface {
    GetID() string
}
```

`GetID` returns a unique string ID for the item. This ID is used in lifecycle events so that individual items can be traced across stages and workers.

The library ships a convenience wrapper for plain strings:

```go
// ID and Value set to the same string
item := pipeline.NewStringItem("hello")

// explicit ID and value
item := pipeline.NewStringItemWithID("order-42", "processed")
```

---

## Configuration

`Config` is the central object that wires together an `Observer`, a `Logger`, and a `context.Context`. It is passed to every stage.

```go
type Config struct {
    Observer Observer
    Logger   Logger
    Context  context.Context
}
```

### Constructors

```go
// Sensible defaults: NoOpObserver, slog.Default(), context.Background()
cfg := pipeline.DefaultConfig()

// Same as above but with a custom *slog.Logger
cfg := pipeline.DefaultConfigWithSlog(logger)
```

### Fluent setters

Each setter returns the same `*Config` pointer, so calls can be chained:

```go
cfg := pipeline.DefaultConfig().
    WithObserver(myObserver).
    WithLogger(myLogger).
    WithContext(ctx)
```

| Method | Description |
|--------|-------------|
| `WithObserver(Observer) *Config` | Replace the observer |
| `WithLogger(Logger) *Config` | Replace the logger |
| `WithContext(context.Context) *Config` | Replace the context |

---

## Stages

A stage reads items from one or more input channels, processes each item concurrently across a configurable worker pool, and writes results to an output channel. When all input channels close, all workers finish, and the output channel is closed automatically.

### StartStage

```go
func StartStage[I Identifier, O Identifier](sc StageConfig[I, O]) (<-chan O, error)
```

The primary stage constructor. Both the input type `I` and the output type `O` must satisfy `Identifier`. Returns an error when no input channels are provided.

Create a `StageConfig` with `NewStageConfig`:

```go
func NewStageConfig[I Identifier, O Identifier](
    cfg      *Config,
    stageId  string,
    workers  int,
    fn       func(I) (O, error),
    channels ...<-chan I,
) StageConfig[I, O]
```

**Parameters**

| Parameter | Description |
|-----------|-------------|
| `cfg` | Pipeline configuration (observer, logger, context) |
| `stageId` | Human-readable name used in events and logs |
| `workers` | Number of concurrent goroutines processing items |
| `fn` | Processing function; returning an error drops the item and emits a `ProcessFailed` event |
| `channels` | One or more input channels; multiple channels are merged automatically |

**Example — two chained stages**

```go
source := make(chan Order, 10)
// ... populate source, then close it

// Stage 1: validate and enrich
enriched, err := pipeline.StartStage(pipeline.NewStageConfig(
    cfg,
    "enrich",
    3,
    func(o Order) (EnrichedOrder, error) {
        if o.Price < 0 {
            return EnrichedOrder{}, fmt.Errorf("negative price for %s", o.ID)
        }
        return EnrichedOrder{Order: o, Total: float64(o.Qty) * o.Price}, nil
    },
    source,
))

// Stage 2: format
formatted, err := pipeline.StartStage(pipeline.NewStageConfig(
    cfg,
    "format",
    2,
    func(e EnrichedOrder) (pipeline.StringItem, error) {
        s := fmt.Sprintf("[%s] total=%.2f", e.ID, e.Total)
        return pipeline.NewStringItemWithID(e.ID, s), nil
    },
    enriched,
))

for item := range formatted {
    fmt.Println(item.Value)
}
```

When multiple input channels are passed, they are merged with `Merge` before distribution to workers. The output channel buffer size equals `workers`.

#### Lifecycle events emitted by StartStage

| Event | Moment |
|-------|--------|
| `StageStarted` | Before workers are launched |
| `WorkerStarted` | When each worker goroutine begins |
| `ProcessStarted` | Before `fn` is called for an item |
| `ProcessCompleted` | After `fn` succeeds |
| `ProcessFailed` | After `fn` returns an error |
| `WorkerCompleted` | When a worker goroutine exits |
| `StageCompleted` | After all workers finish and the output channel closes |

#### Context cancellation

Workers respect the context stored in `Config`. If the context is cancelled, each worker exits at its next opportunity without processing further items.

```go
ctx, cancel := context.WithCancel(context.Background())
cfg := pipeline.DefaultConfig().WithContext(ctx)

out, _ := pipeline.StartStage(pipeline.NewStageConfig(cfg, "my-stage", 4, fn, source))

time.AfterFunc(500*time.Millisecond, cancel) // stop after 500ms

for range out {} // drains until the stage stops
```

---

### StartStageWithErr

```go
func StartStageWithErr[I, O any](
    id      string,
    workers int,
    fn      func(I) (O, error),
    ec      chan error,
    c       ...<-chan I,
) <-chan O
```

A simpler, lighter variant that does **not** require `I` or `O` to satisfy `Identifier` and does **not** use a `Config`. Instead of dropping failed items silently, it sends wrapped errors to the provided error channel `ec`.

- If no input channels are passed, the error is sent to `ec` and `nil` is returned.
- Processing errors are wrapped as `"<id> stage processing error | <original error>"` and sent to `ec`; the item is skipped.
- The output channel is closed once all workers finish.

**Important:** the caller is responsible for closing `ec` (typically after draining the output channel), otherwise a `range ec` loop will block forever.

```go
errCh := make(chan error, 10)

out := pipeline.StartStageWithErr(
    "validate",
    2,
    func(o Order) (string, error) {
        if o.Price < 0 {
            return "", fmt.Errorf("invalid price for %s", o.ID)
        }
        return o.ID + " ok", nil
    },
    errCh,
    source,
)

go func() {
    for result := range out {
        fmt.Println("ok:", result)
    }
    close(errCh) // safe to close after out is drained
}()

for err := range errCh {
    fmt.Println("error:", err)
}
```

---

## Routing

All routing functions run in a background goroutine and close every output channel when the input channel closes.

### ConditionalRoute

```go
func ConditionalRoute[T any](
    input          <-chan T,
    condition      func(T) bool,
    matchBufSize   int,
    nomatchBufSize int,
) (match, nomatch chan T)
```

Splits one channel into two. Items for which `condition` returns `true` go to `match`; all others go to `nomatch`.

```go
expensive, cheap := pipeline.ConditionalRoute(
    orders,
    func(o Order) bool { return o.Price >= 100 },
    10, 10,
)
```

---

### RouteByKey

```go
func RouteByKey[T any, K comparable](
    input      <-chan T,
    bufferSize int,
    keyFunc    func(T) K,
    keys       ...K,
) map[K]chan T
```

Routes each item to the channel that matches the key returned by `keyFunc`. All output channels share the same `bufferSize`. Panics if an item produces a key that was not listed in `keys`.

```go
routes := pipeline.RouteByKey(
    orders,
    5,
    func(o Order) string { return o.Region },
    "eu", "us", "apac",
)

for o := range routes["eu"] { ... }
```

---

### RouteByPredicate

```go
func RouteByPredicate[T any](
    input       <-chan T,
    bufferSizes []int,
    predicates  ...func(T) bool,
) []chan T
```

Routes each item to the **first** predicate that matches it. `bufferSizes` and `predicates` must have the same length. Panics if no predicate matches an item.

```go
tiers := pipeline.RouteByPredicate(
    numbers,
    []int{10, 10, 10},
    func(n int) bool { return n > 100 },  // tiers[0]: high
    func(n int) bool { return n > 10 },   // tiers[1]: medium
    func(n int) bool { return true },      // tiers[2]: catch-all low
)
```

---

### Fork

```go
// Buffered
func Fork[T, U any](c <-chan any, leftBufSize, rightBufSize int) (left chan T, right chan U)

// Unbuffered
func ForkUnbuffered[T, U any](c <-chan any) (left chan T, right chan U)
```

Splits a `chan any` into two typed channels using a type switch. Items of type `T` go to `left`; items of type `U` go to `right`. Panics if an item matches neither type.

```go
ints, strs := pipeline.Fork[int, string](mixed, 10, 10)
```

---

### MultiTypeRoute

```go
func MultiTypeRoute(
    input      <-chan any,
    bufferSize int,
    types      ...reflect.Type,
) map[reflect.Type]chan any
```

Reflection-based dispatch. Creates one output channel per registered type. Panics if an item's runtime type is not registered.

```go
routes := pipeline.MultiTypeRoute(
    input,
    10,
    reflect.TypeFor[int](),
    reflect.TypeFor[string](),
    reflect.TypeFor[float64](),
)

for v := range routes[reflect.TypeFor[int]()] { ... }
```

---

## Utilities

### Merge

```go
func Merge[T any](cs ...<-chan T) <-chan T
```

Fan-in: combines any number of channels of the same type into a single channel. The output channel is closed when all input channels are closed. Items arrive in arrival order (non-deterministic across channels).

```go
merged := pipeline.Merge(ch1, ch2, ch3)
for v := range merged { ... }
```

`StartStage` calls `Merge` automatically when more than one input channel is provided.

---

## Result Type

`Result[S, W, E]` is a generic, three-state value that represents the outcome of an operation without relying on multiple return values.

```go
type Result[S, W, E any] struct { ... } // fields are unexported
```

### Constructors

```go
// Only success is set
pipeline.Success[S, W, E](status S) Result[S, W, E]

// Both success and warning are set
pipeline.Warning[S, W, E](status S, warning W) Result[S, W, E]

// Only error is set
pipeline.Error[S, W, E](status S, err E) Result[S, W, E]
```

### Manage

```go
func Manage[S, W, E, SR, WR, ER any](
    r              Result[S, W, E],
    successHandler func(S) SR,
    warningHandler func(W) WR,
    errorHandler   func(E) ER,
)
```

Calls the matching handler for each non-nil field. Because `Warning` sets both `success` and `warning`, both `successHandler` and `warningHandler` are called for a warning result.

```go
result := pipeline.Warning[string, string, error]("accepted", "low stock")

pipeline.Manage(result,
    func(s string) string { fmt.Println("success:", s); return s },
    func(w string) string { fmt.Println("warning:", w); return w },
    func(e error)  string { fmt.Println("error:",   e); return "" },
)
// prints:
//   success: accepted
//   warning: low stock
```

---

## Observability

### Events

The library emits structured events throughout stage execution. Every event satisfies the `Eventful` interface:

```go
type Eventful interface {
    GetID()       string
    GetType()     EventType
    GetTime()     time.Time
    GetMetadata() map[string]any
}
```

#### Event types

| Constant | String value | Emitted when |
|----------|-------------|--------------|
| `StageStarted` | `"stage_started"` | `StartStage` is called |
| `StageCompleted` | `"stage_finished"` | All workers finish and output closes |
| `WorkerStarted` | `"worker_started"` | A worker goroutine begins |
| `WorkerCompleted` | `"worker_finished"` | A worker goroutine exits |
| `ProcessStarted` | `"item_process_started"` | `fn` is about to be called |
| `ProcessCompleted` | `"item_processed"` | `fn` returned without error |
| `ProcessFailed` | `"item_failed"` | `fn` returned an error |
| `RouterStarted` | `"router_started"` | (available for custom use) |
| `RouterCompleted` | `"router_finished"` | (available for custom use) |
| `RouterFailed` | `"router_error"` | (available for custom use) |

#### Event concrete types

| Type | Extra field | Used for |
|------|------------|---------|
| `*Event` | — | All standard lifecycle events |
| `*ErrorEvent` | `Error error` | Failed processes, router errors |
| `*WarningEvent` | `Warning string` | Soft warnings |
| `*DebugEvent` | `Message string` | Debug messages |

#### Event metadata keys

Metadata is accessible via `GetMetadata()`. The following keys are embedded by the library:

| Key | Present on |
|-----|-----------|
| `"stage-id"` | Worker and process events |
| `"worker-id"` | Process events |
| `"workers"` | `StageStarted` |
| `"input-channel"` | Router started events |
| `"output-channel"` | Router completed events |

---

### Observer

```go
type Observer interface {
    OnEvent(ctx context.Context, event Eventful)
}
```

An `Observer` receives every event emitted by a stage. Attach one to a `Config` via `WithObserver`.

#### Provided implementations

**`NoOpObserver`** — discards all events. Used by `DefaultConfig`.

```go
var _ pipeline.Observer = pipeline.NoOpObserver{}
```

**`ObserverFunc`** — adapts any function to the `Observer` interface.

```go
obs := pipeline.ObserverFunc(func(ctx context.Context, e pipeline.Eventful) {
    fmt.Printf("event: %s id: %s\n", e.GetType(), e.GetID())
})
cfg := pipeline.DefaultConfig().WithObserver(obs)
```

**`MultiObserver`** — broadcasts each event to a list of observers. Safe to add observers to after construction.

```go
multi := pipeline.NewMultiObserver(obs1, obs2)
multi.Add(obs3)
cfg := pipeline.DefaultConfig().WithObserver(multi)
```

**`LoggerObserver`** — routes events to a `Logger` at the appropriate level:
- `*ErrorEvent` → `Error`
- `*WarningEvent` → `Warn`
- `*DebugEvent` → `Debug`
- `*Event` → `Debug`
- anything else → `Info`

```go
logObs := pipeline.NewLoggerObserver(pipeline.NewSlogLogger(slog.Default()))
cfg := pipeline.DefaultConfig().WithObserver(logObs)
```

---

### Metrics

`MetricsCollector` implements `Observer` and accumulates execution metrics across stages, workers, and individual items.

```go
collector := pipeline.NewMetricsCollector(cfg)
defer collector.Close()

cfg = cfg.WithObserver(collector)
```

> `NewMetricsCollector` must be called **before** `WithObserver`. The config passed to `NewMetricsCollector` is used for internal logging; the collector itself becomes the observer on the config used by stages.

#### Design

- **High-frequency events** (`ProcessCompleted`, `ProcessFailed`) update `atomic.Int64` counters — no locks, no contention.
- **Low-frequency structural events** (stage/worker start/stop) are sent to a buffered channel and processed by a single background goroutine.
- A second background goroutine periodically (default: every 100 ms) aggregates per-entity counters into the global `ExecutionMetrics` snapshot.

#### API

```go
// Block until shutdown; final aggregation is performed.
collector.Close() error

// Always up-to-date, no aggregation lag.
processed, failed := collector.GetRealtimeTotals()

// Deep-copy snapshot of all metrics. Totals in BaseMetrics are real-time accurate.
metrics := collector.GetMetrics() // returns ExecutionMetrics

// Change the aggregation interval (default 100ms).
collector.SetAggregationInterval(50 * time.Millisecond)
```

#### ExecutionMetrics structure

```
ExecutionMetrics
├── BaseMetrics          (StartTime, EndTime, Duration, ProcessedCounter, FailedCounter)
├── StagesMetrics        map[stageId → StageMetrics]
│   └── StageMetrics
│       ├── BaseMetrics
│       └── WorkersMetrics  map[workerId → WorkerMetrics]
│           └── WorkerMetrics
│               └── BaseMetrics
└── RoutersMetrics       map[routerId → RouterMetrics]  (reserved for future use)
```

---

### Logging

The library defines its own `Logger` interface so that the internal `*slog.Logger` dependency is not leaked to callers:

```go
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    With(args ...any) Logger
}
```

#### Wrapping slog

```go
// Wrap any *slog.Logger — nil falls back to slog.Default()
logger := pipeline.NewSlogLogger(mySlogLogger)
```

#### Provided logger constructors

All constructors return a `*slog.Logger` from the standard library, which can be wrapped with `NewSlogLogger`.

| Function | Format | Level | Destination |
|----------|--------|-------|-------------|
| `DefaultTextLogger()` | text | Info | stdout |
| `DefaultJSONLogger()` | JSON | Info | stdout |
| `DefaultDebugTextLogger()` | text | Debug | stdout |
| `DefaultDebugJSONLogger()` | JSON | Debug | stdout |
| `NewTextLoggerToFile(path, level)` | text | configurable | file |
| `NewJSONLoggerToFile(path, level)` | JSON | configurable | file |
| `NewLoggerToWriter(w, useJSON, level)` | text/JSON | configurable | any `io.Writer` |
| `NewMultiLogger(useJSON, level, writers...)` | text/JSON | configurable | multiple `io.Writer`s |

#### NoOpLogger

Discards everything. Useful in tests.

```go
var _ pipeline.Logger = pipeline.NoOpLogger{}
```

#### MultiWriter

`NewMultiWriter(writers ...io.Writer)` creates an `io.Writer` that fans out to all given writers, letting you build loggers that write to, for example, both stdout and a file simultaneously.

```go
w := pipeline.NewMultiWriter(os.Stdout, fileWriter)
logger := pipeline.NewLoggerToWriter(w, true, slog.LevelInfo)
```

---

## Type Reference

### Interfaces

| Type | Defined in | Purpose |
|------|-----------|---------|
| `Identifier` | `event.go` | Items flowing through `StartStage` |
| `Eventful` | `event.go` | Structured events emitted by stages |
| `Observer` | `observer.go` | Receives pipeline events |
| `Logger` | `observer.go` | Structured logging abstraction |

### Structs

| Type | Purpose |
|------|---------|
| `Config` | Bundles Observer, Logger, and context for a stage |
| `StageConfig[I, O]` | Parameters for `StartStage`; created via `NewStageConfig` |
| `StringItem` | `Identifier`-compatible string wrapper |
| `Result[S, W, E]` | Three-state operation outcome |
| `MetricsCollector` | Implements `Observer`; collects execution metrics |
| `MultiObserver` | Fan-out observer |
| `LoggerObserver` | Bridges `Observer` events to a `Logger` |
| `ObserverFunc` | Function adapter for `Observer` |
| `NoOpObserver` | Silent observer (default) |
| `NoOpLogger` | Silent logger |
| `MultiWriter` | `io.Writer` that writes to multiple destinations |
| `ExecutionMetrics` | Top-level metrics snapshot returned by `GetMetrics` |
| `StageMetrics` | Per-stage timing and counters |
| `WorkerMetrics` | Per-worker timing and counters |