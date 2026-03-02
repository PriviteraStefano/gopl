# gopl

A Go library for building concurrent data-processing pipelines using channels and goroutines. It provides composable building blocks for stages, routing, fan-in, observability, and result handling — all backed by Go generics.

Requires **Go 1.24+**.

---

## Table of Contents

- [Core Concepts](#core-concepts)
  - [Identifier](#identifier)
  - [Process](#process)
  - [Worker](#worker)
  - [Stage](#stage)
  - [Router](#router)
  - [Pipeline](#pipeline)
- [Configuration](#configuration)
- [Stages](#stages)
  - [StartStage — functional API](#startstage--functional-api)
  - [StageBuilder — fluent API](#stagebuilder--fluent-api)
- [Routing](#routing)
- [PipelineBuilder](#pipelinebuilder)
  - [Registering stages](#registering-stages)
  - [Registering routers](#registering-routers)
  - [Validation](#validation)
  - [Building and running](#building-and-running)
  - [Consuming results](#consuming-results)
  - [Per-component config overrides](#per-component-config-overrides)
  - [Lifecycle management](#lifecycle-management)
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

This library is built around five composable concepts that stack on top of each other: **Process**, **Worker**, **Stage**, **Router** and **Pipeline**. Understanding how they relate makes every other API decision obvious.

```
Pipeline
└── Stage  (named node in the DAG)
    └── Worker  (goroutine, one per concurrency slot)
        └── Process  (single item being transformed)

Router  (named node in the DAG — fans one channel out to many)
```

---

### Process

A **Process** is the smallest unit of work: the act of calling your transformation function `fn(item)` on a **single item** and routing the result (or error) onwards.

```
item ──▶ fn(item) ──▶ output channel   (on success)
                 └──▶ ProcessFailed event  (on error, item is dropped)
```

Concretely, `StartProcess` handles this:

```go
// ProcessConfig bundles everything needed to process one item.
type ProcessConfig[I Identifier, O Identifier] struct {
    WorkerConfig[I, O]
    Item I
}

func StartProcess[I Identifier, O Identifier](pc ProcessConfig[I, O])
```

`StartProcess`:
1. Emits a `ProcessStarted` event (carries `item.GetID()`, worker ID, stage ID).
2. Calls `fn(item)`.
3. On success — emits `ProcessCompleted`, sends the result to the output channel.
4. On error  — emits `ProcessFailed` with the error attached; the item is **silently dropped** (not forwarded).

You rarely call `StartProcess` directly. A `Worker` calls it in a loop.

---

### Worker

A **Worker** is a long-lived goroutine that pulls items from a shared input channel and calls `StartProcess` for each one. Multiple workers within the same stage share the same input channel, so the Go scheduler distributes items naturally.

```
input channel ──▶ Worker 0 ──▶ StartProcess ──▶ output channel
             └──▶ Worker 1 ──▶ StartProcess ──┘
             └──▶ Worker 2 ──▶ StartProcess ──┘
```

```go
// WorkerConfig bundles everything a single worker goroutine needs.
type WorkerConfig[I Identifier, O Identifier] struct {
    StageConfig[I, O]
    WorkerId int
    StageWg  *sync.WaitGroup
    Out      chan O
}

func StartWorker[I Identifier, O Identifier](wc WorkerConfig[I, O])
```

`StartWorker`:
1. Checks the `Config` context **once at startup** — exits immediately if already cancelled.
2. Emits a `WorkerStarted` event.
3. Enters a `for item := range ch` loop, calling `StartProcess` for each item.
4. When the input channel is closed, exits the loop, emits `WorkerCompleted`, and decrements the stage's `WaitGroup`.

> **Important cancellation note:** the context is only checked at the start of the worker loop, not on every iteration. Cancelling the context will stop a worker that is **between items**, but a worker currently **blocked waiting** for the next item will not unblock until a new item arrives or the channel is closed. To fully stop a blocked worker you must both cancel the context **and** close (or drain) the feeder channel.

---

### Stage

A **Stage** is a named, self-contained transformation node. It owns a pool of workers, merges its input channels, and produces one output channel. When every input channel has been closed and all workers have finished, the output channel is closed automatically — propagating the "no more items" signal downstream.

```
<-chan I  ──┐
<-chan I  ──┼──▶ Merge ──▶ [Worker 0]──┐
<-chan I  ──┘              [Worker 1]──┼──▶ chan O (closed when done)
                           [Worker 2]──┘
```

```go
func StartStage[I Identifier, O Identifier](
    cfg     *Config,
    stageId string,
    workers int,
    fn      func(I) (O, error),
    channels ...<-chan I,
) (<-chan O, error)
```

Key behaviours:
- **Fan-in**: multiple input channels are merged transparently via `Merge` before workers pull from them.
- **Fan-out**: `workers` goroutines run concurrently; the output channel buffer size equals `workers`.
- **Backpressure**: workers block when the output channel is full, naturally throttling upstream producers.
- **Error isolation**: a failed item emits a `ProcessFailed` event and is dropped; all other items continue unaffected.
- **Automatic teardown**: a background goroutine waits on the stage's `WaitGroup`, closes the output channel, and emits `StageCompleted` once all workers have exited.

**Lifecycle events emitted by a stage (in order)**

```
StageStarted
  WorkerStarted  (×workers)
    ProcessStarted  (per item)
    ProcessCompleted | ProcessFailed  (per item)
  WorkerCompleted  (×workers, as each exits)
StageCompleted
```

You can also build stages incrementally with the fluent `StageBuilder` or the config-first `NewStageConfig` + `Start()` approach. All three share the same underlying `StartWorker` / `StartProcess` machinery.

---

### Router

A **Router** is a fan-out node that reads from **one** input channel and writes each item to **one** of several named output channels (branches). Every branch is a separate channel; downstream stages subscribe to individual branches by name.

```
              ┌──▶ branch "premium"  <-chan T
input <-chan T ──▶ Router
              └──▶ branch "standard" <-chan T
```

The routing function `fn` receives the input channel and returns a `map[branchKey]<-chan T`. You control the routing logic entirely — you can use `ConditionalRoute`, `RouteByKey`, `RouteByPredicate`, or any custom implementation. The contract is:

- **Every branch channel must eventually be closed** when the input is exhausted. Built-in routers handle this for you; custom implementations must do it manually.
- A router is **type-preserving**: the output type is the same as the input type (`T`). If you need a type transformation, place a stage after the router.

Downstream stages reference a specific branch using a `RouterBranchRef`:

```go
gopl.RouterBranchRef{RouterID: "myRouter", BranchKey: "premium"}.ID()
// returns "myRouter/premium"
```

---

### Pipeline

A **Pipeline** is a directed acyclic graph (DAG) of stages and routers, wired together and managed as a single unit. The `PipelineBuilder` lets you declare nodes and their upstream dependencies by name; it then validates the graph, performs a topological sort, wires all channels, and starts all goroutines in the correct order with a single `Build` call.

```
[feeder] ──▶ [stage A] ──▶ [router] ──▶ [stage B]
                                    └──▶ [stage C]
```

```go
pb := gopl.NewPipelineBuilder(cfg)
gopl.AddStage(pb,  "A",      2, fnA)                              // source: fed externally
gopl.AddRouter(pb, "router", routeFn, "A")                        // fans out A's output
gopl.AddStage(pb,  "B",      2, fnB,
    gopl.RouterBranchRef{RouterID: "router", BranchKey: "x"}.ID())
gopl.AddStage(pb,  "C",      2, fnC,
    gopl.RouterBranchRef{RouterID: "router", BranchKey: "y"}.ID())

handle, err := pb.Build(ctx, map[string]any{
    "A": (<-chan MyType)(feeder),
})
```

`Build` returns a `PipelineHandle` with:
- **`Wait()`** — blocks until every stage has finished.
- **`Cancel()`** — cancels the shared context to signal workers to stop.
- **`Done()`** — a channel closed when every goroutine has exited.
- **`Results()`** — a `PipelineResults` accessor for typed channel retrieval.

The builder validates the DAG before starting anything: it checks for cycles, unknown input references, orphaned nodes, type mismatches on edges, and missing feeder channels.

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
cfg := gopl.DefaultConfig()

// Same as above but with a custom *slog.Logger
cfg := gopl.DefaultConfigWithSlog(logger)
```

### Fluent setters

Each setter returns a **new** `*Config` copy, so the original is never mutated. This makes it safe to derive per-component configs from a shared base at any time — including after goroutines have already started using the original.

```go
cfg := gopl.DefaultConfig().
    WithObserver(myObserver).
    WithLogger(myLogger).
    WithContext(ctx)
```

| Method | Description |
|--------|-------------|
| `WithObserver(Observer) *Config` | Returns a copy with the observer replaced |
| `WithLogger(Logger) *Config` | Returns a copy with the logger replaced |
| `WithContext(context.Context) *Config` | Returns a copy with the context replaced |
| `Clone() *Config` | Explicit shallow copy |

---

## Stages

A stage reads items from one or more input channels, processes each item concurrently across a configurable worker pool, and writes results to an output channel. When all input channels close, all workers finish, and the output channel is closed automatically.

### StartStage — functional API

```go
func StartStage[I Identifier, O Identifier](
    cfg     *Config,
    stageId string,
    workers int,
    fn      func(I) (O, error),
    channels ...<-chan I,
) (<-chan O, error)
```

The primary stage constructor. Both the input type `I` and the output type `O` must satisfy `Identifier`. Returns an error when no input channels are provided.

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
enriched, err := gopl.StartStage(
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
)
if err != nil { /* handle */ }

// Stage 2: format — receives the output channel of stage 1
formatted, err := gopl.StartStage(
    cfg,
    "format",
    2,
    func(e EnrichedOrder) (gopl.StringItem, error) {
        s := fmt.Sprintf("[%s] total=%.2f", e.ID, e.Total)
        return gopl.NewStringItemWithID(e.ID, s), nil
    },
    enriched,
)
if err != nil { /* handle */ }

for item := range formatted {
    fmt.Println(item.Value)
}
```

When multiple input channels are passed, they are merged with `Merge` before distribution to workers. The output channel buffer size equals `workers`.

You can also construct a `StageConfig` separately and start it with `StartStageConfig`:

```go
sc := gopl.NewStageConfig(cfg, "enrich", 3, fn, source)
out, err := gopl.StartStageConfig(sc)

// or, equivalently, using the method form:
out, err := sc.Start()
```

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

Workers respect the context stored in `Config`. If the context is cancelled, each worker exits at its next opportunity without processing further items. Note that a worker currently blocked waiting on an input channel will not unblock until the channel receives a new item or is closed.

```go
ctx, cancel := context.WithCancel(context.Background())
cfg := gopl.DefaultConfig().WithContext(ctx)

out, _ := gopl.StartStage(cfg, "my-stage", 4, fn, source)

time.AfterFunc(500*time.Millisecond, cancel) // stop after 500ms

for range out {} // drains until the stage stops
```

---

### StageBuilder — fluent API

`StageBuilder` provides a fluent, step-by-step alternative to `StartStage` when you prefer to assemble stage parameters incrementally or in separate parts of your code.

```go
sc, err := gopl.NewStageBuilder[Order, EnrichedOrder]().
    WithConfig(cfg).
    WithStageId("enrich").
    WithWorkers(3).
    WithFunction(func(o Order) (EnrichedOrder, error) {
        return EnrichedOrder{Order: o, Total: float64(o.Qty) * o.Price}, nil
    }).
    WithChannels(source).
    Build()
if err != nil { /* handle */ }

out, err := sc.Start()
```

**Builder methods**

| Method | Description |
|--------|-------------|
| `WithConfig(cfg *Config)` | Set the pipeline config |
| `WithStageId(id string)` | Set the stage name |
| `WithWorkers(n int)` | Set the worker count |
| `WithFunction(fn func(I)(O,error))` | Set the processing function |
| `WithChannels(channels ...<-chan I)` | Replace the channel list |
| `AddChannel(ch <-chan I)` | Append a single channel |
| `AddChannels(chs ...<-chan I)` | Append multiple channels |
| `Build() (*StageConfig[I,O], error)` | Validate and produce a `StageConfig` |

`Build` returns an error if any required field is missing: config, stage ID, workers, function, or at least one channel. On success it returns a `*StageConfig` whose `Start()` method launches the stage.

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
expensive, cheap := gopl.ConditionalRoute(
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
routes := gopl.RouteByKey(
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
tiers := gopl.RouteByPredicate(
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
ints, strs := gopl.Fork[int, string](mixed, 10, 10)
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
routes := gopl.MultiTypeRoute(
    input,
    10,
    reflect.TypeFor[int](),
    reflect.TypeFor[string](),
    reflect.TypeFor[float64](),
)

for v := range routes[reflect.TypeFor[int]()] { ... }
```

---

## PipelineBuilder

`PipelineBuilder` is the high-level orchestration layer. Rather than manually wiring output channels to input channels, you describe the shape of your pipeline as a **directed acyclic graph (DAG)** of named stages and routers. The builder validates the graph, performs a topological sort, wires all channels and starts all goroutines in the correct order with a single `Build` call.

Because Go generics cannot be attached to methods, stage and router registrations use **package-level generic functions** that accept the builder as their first argument.

**Quick overview**

```
                                    ┌──▶ [premium]  ──▶ results
[feeder] ──▶ [enrich] ──▶ [split] ──┤
                                    └──▶ [standard] ──▶ results
```

```go
pb := gopl.NewPipelineBuilder(cfg)

gopl.AddStage(pb, "enrich",   2, enrichFn)
gopl.AddRouter(pb, "split",   splitFn,      "enrich")
gopl.AddStage(pb, "premium",  2, premiumFn,
    gopl.RouterBranchRef{RouterID: "split", BranchKey: "premium"}.ID())
gopl.AddStage(pb, "standard", 2, standardFn,
    gopl.RouterBranchRef{RouterID: "split", BranchKey: "standard"}.ID())

handle, err := pb.Build(ctx, map[string]any{
    "enrich": (<-chan Order)(feeder),
})

premiumResults, _  := gopl.CollectAll[ProcessedOrder](handle.Results(), "premium")
standardResults, _ := gopl.CollectAll[ProcessedOrder](handle.Results(), "standard")
```

> The builders block can't be extension methods due to the way Go handles method receivers and type parameters.

---

### Registering stages

```go
func AddStage[I Identifier, O Identifier](
    pb       *PipelineBuilder,
    id       string,
    workers  int,
    fn       func(I) (O, error),
    inputIDs ...string,
) *PipelineBuilder
```

Registers a typed stage. `inputIDs` are the IDs of upstream components whose output channels should feed this stage:

- **Omit `inputIDs`** for **source stages** — stages that have no upstream component and are instead fed by an external channel supplied to `Build` via the `feeders` map.
- **List one or more component IDs** for intermediate and sink stages.
- **Use `RouterBranchRef.ID()`** to consume a specific branch of a router output (see [Registering routers](#registering-routers)).

When multiple `inputIDs` are given the corresponding output channels are merged before being distributed to the stage's workers.

```go
// Source stage — no upstream, fed by a feeder at Build time
gopl.AddStage(pb, "parse", 2, parseFn)

// Intermediate stage — receives output of "parse"
gopl.AddStageAfter(pb, "enrich", 4, enrichFn, "parse")

// Fan-in sink — receives output of both "left" and "right"
gopl.AddStage(pb, "merge", 2, mergeFn, "left", "right")
```

`AddStageAfter` is a convenience alias for the single-upstream case:

```go
func AddStageAfter[I Identifier, O Identifier](
    pb      *PipelineBuilder,
    id      string,
    workers int,
    fn      func(I) (O, error),
    afterID string,
) *PipelineBuilder
```

---

### Registering routers

```go
func AddRouter[T any](
    pb      *PipelineBuilder,
    id      string,
    fn      func(<-chan T) map[string]<-chan T,
    inputID string,
) *PipelineBuilder
```

Registers a router. The router function `fn` receives the upstream output channel and returns a map of **branch key → output channel**. You are responsible for closing those output channels when the input closes.

```go
splitFn := func(in <-chan EnrichedOrder) map[string]<-chan EnrichedOrder {
    premium  := make(chan EnrichedOrder, 16)
    standard := make(chan EnrichedOrder, 16)
    go func() {
        defer close(premium)
        defer close(standard)
        for o := range in {
            if o.Total >= 500 {
                premium <- o
            } else {
                standard <- o
            }
        }
    }()
    return map[string]<-chan EnrichedOrder{
        "premium":  premium,
        "standard": standard,
    }
}

gopl.AddRouter(pb, "split", splitFn, "enrich")
```

Downstream stages consume a specific branch by using `RouterBranchRef`:

```go
type RouterBranchRef struct {
    RouterID  string
    BranchKey string
}

// Returns "<RouterID>/<BranchKey>", e.g. "split/premium"
func (r RouterBranchRef) ID() string
```

```go
gopl.AddStage(pb, "handlePremium", 2, premiumFn,
    gopl.RouterBranchRef{RouterID: "split", BranchKey: "premium"}.ID())

gopl.AddStage(pb, "handleStandard", 2, standardFn,
    gopl.RouterBranchRef{RouterID: "split", BranchKey: "standard"}.ID())
```

`AddRouterAfter` is an alias that makes the single upstream explicit:

```go
gopl.AddRouterAfter(pb, "split", splitFn, "enrich")
```

---

### Validation

`Validate()` inspects the DAG for correctness and returns a `ValidationResult` that **accumulates every problem found** — not just the first.

```go
vr := pb.Validate()
if !vr.IsValid {
    fmt.Fprintln(os.Stderr, vr.String())
    os.Exit(1)
}
```

The following checks are performed in order:

| # | Check | Example error |
|---|-------|--------------|
| 1 | **Registration errors** — duplicate component IDs | `component "enrich" already registered` |
| 2 | **Non-empty graph** | `pipeline has no components` |
| 3 | **Worker count ≥ 1** for every stage | `component "enrich": workers must be ≥ 1, got 0` |
| 4 | **All `inputIDs` resolve** — each must be a known component ID or a valid `"routerID/branchKey"` reference | `component "persist": references unknown inputID "ghost"` |
| 5 | **No cycles** — DAG check | `cycle detected: A → B → A` |
| 6 | **Type compatibility** — output type of upstream matches input type of downstream | `component "stage2": type mismatch: upstream "stage1" outputs MyInput, but this component expects MyOutput` |
| 7 | **No orphaned components** — every node is reachable from a source or can reach a sink | `component "lonely": orphaned component: not reachable from any source or sink` |

`Build` always calls `Validate` internally and returns a descriptive error if it fails, so calling `Validate` explicitly beforehand is optional but recommended for early feedback.

```go
type ValidationResult struct {
    IsValid bool
    Errors  []ValidationError
}

type ValidationError struct {
    Component string // empty for graph-level errors (e.g. cycles)
    Reason    string
}

func (e ValidationError) Error() string
func (vr ValidationResult) String() string  // multi-line summary
```

---

### Building and running

```go
func (pb *PipelineBuilder) Build(
    ctx     context.Context,
    feeders map[string]any,
) (*PipelineHandle, error)
```

`Build` validates the DAG, topologically sorts all components, and starts them in dependency order. It returns a `*PipelineHandle` through which results can be consumed and the pipeline lifecycle managed.

**`feeders`** must contain one entry per source stage — a stage with no registered `inputIDs`. The value must be a receive-direction channel (`<-chan T`) whose element type matches the stage's declared input type `I`:

```go
feeder := make(chan Order, len(orders))
for _, o := range orders { feeder <- o }
close(feeder)

handle, err := pb.Build(ctx, map[string]any{
    "parse": (<-chan Order)(feeder), // explicit receive-direction conversion
})
```

If `feeders` is `nil`, `Build` only succeeds for pipelines that have no source stages (all stages have at least one upstream component already wired through the DAG).

If any component fails to start, `Build` cancels all already-started goroutines and returns a non-nil error.

---

### Consuming results

`Build` returns a `*PipelineHandle`. Call `.Results()` on it to obtain a `*PipelineResults` accessor with the following package-level generic functions:

#### GetStageOutput

```go
func GetStageOutput[T any](pr *PipelineResults, stageID string) (<-chan T, error)
```

Returns the typed output channel for a stage. The channel is closed when the stage finishes. Returns an error if the stage ID is unknown or if `T` does not match the stage's actual output type.

```go
ch, err := gopl.GetStageOutput[EnrichedOrder](handle.Results(), "enrich")
for item := range ch {
    fmt.Println(item.ID, item.Total)
}
```

#### GetRouterBranch

```go
func GetRouterBranch[T any](pr *PipelineResults, routerID, branchKey string) (<-chan T, error)
```

Returns the typed output channel for a specific router branch.

```go
premiumCh, err := gopl.GetRouterBranch[EnrichedOrder](handle.Results(), "split", "premium")
```

#### GetMergedOutput

```go
func GetMergedOutput[T any](pr *PipelineResults, stageIDs ...string) (<-chan T, error)
```

Merges the output channels of multiple stages into a single channel. The merged channel closes once all named stages have finished.

```go
allResults, err := gopl.GetMergedOutput[ProcessedOrder](
    handle.Results(), "premium", "standard",
)
for item := range allResults { ... }
```

#### CollectAll

```go
func CollectAll[T any](pr *PipelineResults, stageID string) ([]T, error)
```

Drains the output channel of a stage into a slice. **Blocks** until the stage finishes and its channel closes.

```go
results, err := gopl.CollectAll[ProcessedOrder](handle.Results(), "persist")
```

---

### Per-component config overrides

Each component can use a different `*Config` — for example, a different observer or logger per stage:

```go
// Apply to a single component
pb.WithComponentConfig("enrich", enrichCfg)

// Apply the same config to several components at once
pb.WithComponentConfigs(debugCfg, "parse", "enrich", "persist")

// Replace the default for all components that have no explicit override
pb.WithDefaultConfig(newDefault)
```

Config resolution order (highest priority first):

1. Per-component override set via `WithComponentConfig`
2. Builder-wide default set via `WithDefaultConfig` (or provided to `NewPipelineBuilder`)
3. Package `DefaultConfig()`

---

### Lifecycle management

`Build` returns a `*PipelineHandle` that exposes three lifecycle primitives:

```go
// Block until all stages have finished.
handle.Wait()

// Signal all stages to stop (cancels the pipeline context).
// Workers exit at their next context-check point; those blocked on a channel
// receive exit only after the channel delivers an item or is closed.
handle.Cancel()

// Channel closed when all stages have finished.
// Useful in a select statement.
select {
case <-handle.Done():
    fmt.Println("pipeline finished")
case <-time.After(10 * time.Second):
    handle.Cancel()
}
```

---

## Utilities

### Identifier

Every item that flows through a pipeline must satisfy the `Identifier` interface:

```go
type Identifier interface {
    GetID() string
}
```

`GetID` returns a unique string ID for the item. This ID is attached to every lifecycle event, so individual items can be traced as they move through stages and workers.

The library ships a convenience wrapper for plain strings:

```go
// ID and Value set to the same string
item := gopl.NewStringItem("hello")

// explicit ID and value
item := gopl.NewStringItemWithID("order-42", "processed")
```


### Merge

```go
func Merge[T any](cs ...<-chan T) <-chan T
```

Fan-in: combines any number of channels of the same type into a single channel. The output channel is closed when all input channels are closed. Items arrive in arrival order (non-deterministic across channels).

```go
merged := gopl.Merge(ch1, ch2, ch3)
for v := range merged { ... }
```

`StartStage` and `PipelineBuilder` both call `Merge` automatically when a stage receives more than one input channel.

---

## Result Type

`Result[S, W, E]` is a generic, three-state value that represents the outcome of an operation without relying on multiple return values.

```go
type Result[S, W, E any] struct { ... } // fields are unexported
```

### Constructors

```go
// Only success is set
gopl.Success[S, W, E](status S) Result[S, W, E]

// Both success and warning are set
gopl.Warning[S, W, E](status S, warning W) Result[S, W, E]

// Only error is set
gopl.Error[S, W, E](status S, err E) Result[S, W, E]
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
result := gopl.Warning[string, string, error]("accepted", "low stock")

gopl.Manage(result,
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
| `StageStarted` | `"stage_started"` | `StartStage` / `Build` starts a stage |
| `StageCompleted` | `"stage_finished"` | All workers finish and output closes |
| `WorkerStarted` | `"worker_started"` | A worker goroutine begins |
| `WorkerCompleted` | `"worker_finished"` | A worker goroutine exits |
| `ProcessStarted` | `"item_process_started"` | `fn` is about to be called |
| `ProcessCompleted` | `"item_processed"` | `fn` returned without error |
| `ProcessFailed` | `"item_failed"` | `fn` returned an error |
| `RouterStarted` | `"router_started"` | `PipelineBuilder` starts a router |
| `RouterCompleted` | `"router_finished"` | Router's `fn` has returned its branch map |
| `RouterFailed` | `"router_error"` | Available for custom router implementations |

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
| `"input-channel"` | `RouterStarted` |
| `"output-channel"` | `RouterCompleted` |

---

### Observer

```go
type Observer interface {
    OnEvent(ctx context.Context, event Eventful)
}
```

An `Observer` receives every event emitted by a stage or router. Attach one to a `Config` via `WithObserver`.

#### Provided implementations

**`NoOpObserver`** — discards all events. Used by `DefaultConfig`.

```go
var _ gopl.Observer = gopl.NoOpObserver{}
```

**`ObserverFunc`** — adapts any function to the `Observer` interface.

```go
obs := gopl.ObserverFunc(func(ctx context.Context, e gopl.Eventful) {
    fmt.Printf("event: %s id: %s\n", e.GetType(), e.GetID())
})
cfg := gopl.DefaultConfig().WithObserver(obs)
```

**`MultiObserver`** — broadcasts each event to a list of observers.

```go
multi := gopl.NewMultiObserver(obs1, obs2)
multi.Add(obs3)
cfg := gopl.DefaultConfig().WithObserver(multi)
```

**`LoggerObserver`** — routes events to a `Logger` at the appropriate level:
- `*ErrorEvent` → `Error`
- `*WarningEvent` → `Warn`
- `*DebugEvent` → `Debug`
- `*Event` → `Debug`
- anything else → `Info`

```go
logObs := gopl.NewLoggerObserver(gopl.NewSlogLogger(slog.Default()))
cfg := gopl.DefaultConfig().WithObserver(logObs)
```

Combining `LoggerObserver` with `MetricsCollector` via `MultiObserver` is the recommended full-observability setup:

```go
baseCfg  := gopl.DefaultConfig()
metrics  := gopl.NewMetricsCollector(baseCfg)
logObs   := gopl.NewLoggerObserver(gopl.NewSlogLogger(slog.Default()))
multi    := gopl.NewMultiObserver(logObs, metrics)
cfg      := baseCfg.WithObserver(multi)
```

---

### Metrics

`MetricsCollector` implements `Observer` and accumulates execution metrics across stages, workers, and individual items.

```go
baseCfg   := gopl.DefaultConfig()
collector := gopl.NewMetricsCollector(baseCfg)
defer collector.Close()

cfg := baseCfg.WithObserver(collector)
```

> `NewMetricsCollector` must be called **before** `WithObserver`. The config passed to `NewMetricsCollector` is used for internal logging only; the collector itself is then set as the observer on the config used by the stages.

#### Design

- **High-frequency events** (`ProcessCompleted`, `ProcessFailed`) update `atomic.Int64` counters — no locks, no contention.
- **Low-frequency structural events** (stage/worker start/stop) are sent to a buffered channel and processed by a single background goroutine.
- A second background goroutine periodically (default: every 100 ms) aggregates per-entity counters into the global `ExecutionMetrics` snapshot.

#### API

```go
// Shut down background goroutines; performs a final aggregation.
collector.Close() error

// Always up-to-date totals, no aggregation lag.
processed, failed := collector.GetRealtimeTotals()

// Deep-copy snapshot of all metrics.
// Totals in BaseMetrics are real-time accurate even between aggregation cycles.
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
logger := gopl.NewSlogLogger(mySlogLogger)
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
var _ gopl.Logger = gopl.NoOpLogger{}
```

#### MultiWriter

`NewMultiWriter(writers ...io.Writer)` creates an `io.Writer` that fans out to all given writers, letting you build loggers that write to, for example, both stdout and a file simultaneously.

```go
w := gopl.NewMultiWriter(os.Stdout, fileWriter)
logger := gopl.NewLoggerToWriter(w, true, slog.LevelInfo)
```

---

## Type Reference

### Interfaces

| Type | Defined in | Purpose |
|------|-----------|---------|
| `Identifier` | `event.go` | Items flowing through `StartStage` |
| `Eventful` | `event.go` | Structured events emitted by stages and routers |
| `Observer` | `observer.go` | Receives pipeline events |
| `Logger` | `observer.go` | Structured logging abstraction |

### Structs

| Type | Purpose |
|------|---------|
| `Config` | Bundles Observer, Logger, and context for a stage or pipeline |
| `StageConfig[I, O]` | Parameters for `StartStage`; produced by `NewStageConfig` or `StageBuilder.Build` |
| `StageBuilder[I, O]` | Fluent builder for a single `StageConfig` |
| `PipelineBuilder` | DAG-based orchestration layer for multi-stage pipelines |
| `PipelineHandle` | Live handle returned by `PipelineBuilder.Build`; exposes `Wait`, `Cancel`, `Done`, `Results` |
| `PipelineResults` | Typed accessor for stage and router output channels |
| `RouterBranchRef` | Identifies a specific router branch; `.ID()` produces `"routerID/branchKey"` |
| `ValidationResult` | Holds the outcome of `PipelineBuilder.Validate` |
| `ValidationError` | Single problem found during DAG validation |
| `StringItem` | `Identifier`-compatible string wrapper |
| `Result[S, W, E]` | Three-state operation outcome |
| `MetricsCollector` | Implements `Observer`; collects execution metrics |
| `ExecutionMetrics` | Top-level metrics snapshot returned by `GetMetrics` |
| `StageMetrics` | Per-stage timing and counters |
| `WorkerMetrics` | Per-worker timing and counters |
| `MultiObserver` | Fan-out observer |
| `LoggerObserver` | Bridges `Observer` events to a `Logger` |
| `ObserverFunc` | Function adapter for `Observer` |
| `NoOpObserver` | Silent observer (default) |
| `NoOpLogger` | Silent logger |
| `MultiWriter` | `io.Writer` that writes to multiple destinations |
