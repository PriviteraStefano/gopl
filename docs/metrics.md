# MetricsCollector Summary

## High-Level Architecture

The `MetricsCollector` uses a **hybrid approach** to balance high throughput with acceptable eventual consistency:

```
┌─────────────────────────────────────────────────────────────┐
│                    MetricsCollector                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐        ┌──────────────────┐           │
│  │  Fast Path       │        │  Slow Path       │           │
│  │  (Lock-Free)     │        │  (Channeled)     │           │
│  ├──────────────────┤        ├──────────────────┤           │
│  │ ItemProcessed    │        │ StageStarted     │           │
│  │ ItemFailed       │        │ StageCompleted   │           │
│  │                  │        │ WorkerStarted    │           │
│  │ Uses: atomics    │        │ WorkerCompleted  │           │
│  │                  │        │                  │           │
│  └──────────────────┘        │ Uses: channel    │           │
│                              │                  │           │
│                              └──────────────────┘           │
│                                                             │
│  ┌──────────────────────────────────────────────┐           │
│  │         Periodic Aggregation                 │           │
│  │         (Every 100ms default)                │           │
│  │  Copies atomic counters → global metrics     │           │
│  └──────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. **Three-Tier Storage**

```
Global Metrics (globalMetrics)
├─ Read with RWMutex
├─ Updated only during aggregation
└─ Provides consistent snapshots

Local Counters (sync.Map)
├─ Per-stage counters (stageCounters)
├─ Per-worker counters (workerCounters)  
├─ Per-router counters (routerCounters)
└─ Lock-free atomic increments

Global Totals (atomic.Int64)
├─ totalProcessed
└─ totalFailed
└─ Always real-time accurate
```

### 2. **Two Processing Paths**

**Fast Path (High-Frequency Events):**
- Events: `ItemProcessed`, `ItemFailed`
- Processing: Direct atomic increments
- No locks, no channels
- Updates 3 places atomically:
  1. Global totals (`totalProcessed`, `totalFailed`)
  2. Stage counters (if `stageID` in metadata)
  3. Worker counters (if `workerID` in metadata)

**Slow Path (Structural Events):**
- Events: `StageStarted/Completed`, `WorkerStarted/Completed`, `RouterStarted/Completed`
- Processing: Queued to channel → single processor goroutine
- Uses mutex (acceptable since infrequent)
- Creates/updates entity structures

### 3. **Two Background Goroutines**

**Goroutine 1: `processStructuralEvents()`**
- Consumes from `structuralEvents` channel
- Handles lifecycle events sequentially
- Single-threaded access to global metrics (no contention)
- Creates entity entries (stages, workers, routers)
- Updates timestamps and durations

**Goroutine 2: `periodicAggregation()`**
- Runs every 100ms (configurable)
- Copies atomic counter values to global metrics
- This is when breakdown metrics get updated
- Uses mutex to ensure consistent snapshot

## Data Flow

### High-Frequency Item Processing

```
Event: ItemProcessed
  │
  ├─> totalProcessed.Add(1)           // Real-time global total
  │
  ├─> Extract "stageID" from metadata
  │   └─> stageCounters[stageID].processed.Add(1)
  │
  └─> Extract "workerID" from metadata
      └─> workerCounters[workerID].processed.Add(1)

All operations are atomic, no locks
```

### Low-Frequency Structural Events

```
Event: StageStarted
  │
  └─> Queue to structuralEvents channel
        │
        └─> processStructuralEvents() goroutine
              │
              └─> Lock globalMu
                  Create StageMetrics entry
                  Unlock globalMu
```

### Aggregation Flow

```
Every 100ms:
  │
  └─> aggregate()
        │
        ├─> Lock globalMu
        │
        ├─> For each stage:
        │     Copy atomic counter → globalMetrics.StagesMetrics[id]
        │
        ├─> For each worker:
        │     Copy atomic counter → globalMetrics.StagesMetrics[parent].WorkersMetrics[id]
        │
        ├─> For each router:
        │     Copy atomic counter → globalMetrics.RoutersMetrics[id]
        │
        └─> Unlock globalMu
```

## Key Design Decisions

### 1. **Why Hybrid Approach?**
- **Problem**: Mutex on every item = bottleneck
- **Solution**: Atomics for counters, channel for structure
- **Result**: Millions of items/sec throughput

### 2. **Why Eventual Consistency?**
- **Trade-off**: Real-time totals vs. detailed breakdowns
- **Decision**: Totals always accurate, breakdowns lag 100ms
- **Reason**: Users care more about "total items processed" than "exact items per worker right now"

### 3. **Why Two Goroutines?**
- **Separation of concerns**:
  - One handles structure (stages/workers lifecycle)
  - One handles aggregation (counter → metrics)
- **Avoids deadlocks**: No nested locks

### 4. **Why `sync.Map`?**
- **Problem**: Don't know worker IDs ahead of time
- **Solution**: `sync.Map` for dynamic key insertion
- **Alternative**: Regular map + mutex would work but slightly slower

## Performance Characteristics

| Operation | Path | Synchronization | Latency | Throughput |
|-----------|------|----------------|---------|------------|
| Record processed item | Fast | Atomic | ~50ns | Millions/sec |
| Record failed item | Fast | Atomic | ~50ns | Millions/sec |
| Stage start/end | Slow | Channel + Mutex | ~10µs | Thousands/sec |
| Get total counts | Query | Atomic load | ~10ns | N/A |
| Get full metrics | Query | RWMutex read | ~1µs | N/A |

## Accuracy Guarantees

| Metric | Accuracy | Staleness |
|--------|----------|-----------|
| `totalProcessed` | 100% | 0ms (real-time) |
| `totalFailed` | 100% | 0ms (real-time) |
| Stage processed count | 100% | ≤100ms |
| Worker processed count | 100% | ≤100ms |
| Stage/Worker lifecycle | 100% | ~10µs (channel latency) |

## Usage Pattern

```go
// Initialize
collector := NewMetricsCollector(config)
defer collector.Close()

// Events flow in automatically via Observer pattern
// ...

// Query real-time totals (always accurate)
processed, failed := collector.GetRealtimeTotals()

// Query full metrics (breakdowns may lag up to 100ms)
snapshot := collector.GetMetrics()

// Adjust staleness tolerance
collector.SetAggregationInterval(200 * time.Millisecond)
```

## Graceful Shutdown

On `Close()`:
1. Cancel context → stops both goroutines
2. Drain `structuralEvents` channel
3. Final aggregation run
4. Wait for goroutines via `WaitGroup`
5. Close channel

This ensures no events are lost and final metrics are complete.

---

**TL;DR**: Fast atomic counters for item processing, channeled structural events, periodic aggregation every 100ms. Global totals are always real-time accurate, detailed breakdowns lag slightly. Designed for million items/sec throughput.
