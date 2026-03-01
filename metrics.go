package gopl

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

type Identity struct {
	ID string `json:"id"`
}

type BaseMetrics struct {
	StartTime        time.Time     `json:"start_time"`
	EndTime          time.Time     `json:"end_time"`
	Duration         time.Duration `json:"duration"`
	ProcessedCounter int64         `json:"processed_counter"`
	FailedCounter    int64         `json:"failed_counter"`
}

// ExecutionMetrics holds metrics about pipeline execution
type ExecutionMetrics struct {
	BaseMetrics
	StagesMetrics  map[string]StageMetrics  `json:"stage_metrics"`
	RoutersMetrics map[string]RouterMetrics `json:"routers_metrics"`
}

// RouterMetrics represents metrics for a single router
type RouterMetrics struct {
	Identity
	BaseMetrics
	PathCounter map[string]int64 `json:"path_counter"`
	Errors      map[string]error `json:"errors"`
}

// StageMetrics represents metrics for a single stage
type StageMetrics struct {
	Identity
	BaseMetrics
	WorkersMetrics map[string]WorkerMetrics `json:"workers_metrics"`
}

// WorkerMetrics represents metrics for a single worker
type WorkerMetrics struct {
	Identity
	BaseMetrics
	ProcessesMetrics map[string]ProcessMetrics `json:"processes_metrics"`
}

type ProcessMetrics struct {
	Identity
	BaseMetrics
}

// LocalCounters holds atomic counters for lock-free updates
type LocalCounters struct {
	processed atomic.Int64
	failed    atomic.Int64
	startTime time.Time
}

// StructuralEvent represents lifecycle events (start/stop)
type StructuralEvent struct {
	Type      EventType
	ID        string
	ParentID  string
	Timestamp time.Time
	Data      map[string]any
}

// MetricsCollector collects and tracks pipeline execution metrics
type MetricsCollector struct {
	// Global metrics (read-only except during aggregation)
	globalMetrics *ExecutionMetrics
	globalMu      sync.RWMutex

	// Local counters per entity (lock-free)
	stageCounters  sync.Map // map[string]*LocalCounters
	workerCounters sync.Map // map[string]*LocalCounters
	routerCounters sync.Map // map[string]*LocalCounters

	// Global atomic totals (always accurate, real-time)
	totalProcessed atomic.Int64
	totalFailed    atomic.Int64

	// Structural events channel
	structuralEvents chan StructuralEvent

	// Aggregation control
	aggregationInterval time.Duration
	aggregationTicker   *time.Ticker

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	config *Config
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(config *Config) *MetricsCollector {
	ctx, cancel := context.WithCancel(context.Background())

	mc := &MetricsCollector{
		globalMetrics: &ExecutionMetrics{
			BaseMetrics:    BaseMetrics{StartTime: time.Now()},
			StagesMetrics:  make(map[string]StageMetrics),
			RoutersMetrics: make(map[string]RouterMetrics),
		},
		structuralEvents:    make(chan StructuralEvent, 1000),
		aggregationInterval: 100 * time.Millisecond,
		aggregationTicker:   time.NewTicker(100 * time.Millisecond),
		ctx:                 ctx,
		cancel:              cancel,
		config:              config,
	}

	// Start background workers
	mc.wg.Add(2)
	go mc.processStructuralEvents()
	go mc.periodicAggregation()

	return mc
}

// Helper function to extract string from metadata
func getMetadataString(metadata map[string]any, key string) string {
	if val, ok := metadata[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// OnEvent implements the Observer interface to collect metrics from events
func (mc *MetricsCollector) OnEvent(ctx context.Context, event Eventful) {
	eventType := event.GetType()
	metadata := event.GetMetadata()

	switch eventType {
	// High-frequency events: update atomic counters (lock-free, instant)
	case ProcessCompleted:
		mc.recordProcessed(event.GetID(), metadata)
	case ProcessFailed:
		mc.recordFailed(event.GetID(), metadata)

	// Low-frequency structural events: send to channel
	case StageStarted, StageCompleted,
		WorkerStarted, WorkerCompleted,
		RouterStarted, RouterCompleted, RouterFailed:
		mc.sendStructuralEvent(ctx, event)
	}
}

// recordProcessed handles item processing (lock-free, high-frequency)
func (mc *MetricsCollector) recordProcessed(id string, metadata map[string]any) {
	// Update global total (always accurate)
	mc.totalProcessed.Add(1)

	// Update stage-local counter if stageID is in metadata
	if stageID := getMetadataString(metadata, stageIDKey); stageID != "" {
		counters := mc.getOrCreateStageCounters(stageID)
		counters.processed.Add(1)
	}

	// Update worker-local counter if workerID is in metadata
	if workerID := getMetadataString(metadata, workerIDKey); workerID != "" {
		counters := mc.getOrCreateWorkerCounters(workerID)
		counters.processed.Add(1)
	}
}

// recordFailed handles item failures (lock-free, high-frequency)
func (mc *MetricsCollector) recordFailed(id string, metadata map[string]any) {
	mc.totalFailed.Add(1)

	if stageID := getMetadataString(metadata, stageIDKey); stageID != "" {
		counters := mc.getOrCreateStageCounters(stageID)
		counters.failed.Add(1)
	}

	if workerID := getMetadataString(metadata, workerIDKey); workerID != "" {
		counters := mc.getOrCreateWorkerCounters(workerID)
		counters.failed.Add(1)
	}
}

// sendStructuralEvent sends low-frequency events to processing channel
func (mc *MetricsCollector) sendStructuralEvent(ctx context.Context, event Eventful) {
	metadata := event.GetMetadata()

	structEvent := StructuralEvent{
		Type:      event.GetType(),
		ID:        event.GetID(),
		ParentID:  getParentId(event),
		Timestamp: event.GetTime(),
		Data:      metadata,
	}

	select {
	case mc.structuralEvents <- structEvent:
		// Event queued successfully
	case <-ctx.Done():
		// Context cancelled, drop event
	case <-mc.ctx.Done():
		// Collector shutting down
	default:
		// Channel full - this is rare for structural events
		if mc.config.Logger != nil {
			mc.config.Logger.Warn("Structural event channel full, dropping event")
		}
	}
}

func getParentId(event Eventful) string {
	var parentId string
	switch event.GetType() {
	case WorkerStarted, WorkerCompleted:
		parentId = getMetadataString(event.GetMetadata(), stageIDKey)
	case ProcessStarted, ProcessFailed, ProcessCompleted:
		parentId = getMetadataString(event.GetMetadata(), workerIDKey)
	default:
		parentId = ""
	}
	return parentId
}

// processStructuralEvents handles lifecycle events in a single goroutine
func (mc *MetricsCollector) processStructuralEvents() {
	defer mc.wg.Done()

	for {
		select {
		case event := <-mc.structuralEvents:
			mc.handleStructuralEvent(event)
		case <-mc.ctx.Done():
			// Drain remaining events
			for len(mc.structuralEvents) > 0 {
				mc.handleStructuralEvent(<-mc.structuralEvents)
			}
			return
		}
	}
}

// handleStructuralEvent processes a single structural event
func (mc *MetricsCollector) handleStructuralEvent(event StructuralEvent) {
	mc.globalMu.Lock()
	defer mc.globalMu.Unlock()

	switch event.Type {
	case StageStarted:
		mc.globalMetrics.StagesMetrics[event.ID] = StageMetrics{
			Identity:       Identity{ID: event.ID},
			BaseMetrics:    BaseMetrics{StartTime: event.Timestamp},
			WorkersMetrics: make(map[string]WorkerMetrics),
		}
		mc.getOrCreateStageCounters(event.ID)

	case StageCompleted:
		if sm, exists := mc.globalMetrics.StagesMetrics[event.ID]; exists {
			sm.EndTime = event.Timestamp
			sm.Duration = sm.EndTime.Sub(sm.StartTime)
			mc.globalMetrics.StagesMetrics[event.ID] = sm
		}

	case WorkerStarted:
		parentID := event.ParentID
		if parentID == "" {
			parentID = getMetadataString(event.Data, stageIDKey)
		}

		if sm, exists := mc.globalMetrics.StagesMetrics[parentID]; exists {
			sm.WorkersMetrics[event.ID] = WorkerMetrics{
				Identity:         Identity{ID: event.ID},
				BaseMetrics:      BaseMetrics{StartTime: event.Timestamp},
				ProcessesMetrics: make(map[string]ProcessMetrics),
			}
			mc.globalMetrics.StagesMetrics[parentID] = sm
		}
		mc.getOrCreateWorkerCounters(event.ID)

	case WorkerCompleted:
		parentID := event.ParentID
		if parentID == "" {
			parentID = getMetadataString(event.Data, stageIDKey)
		}

		if sm, exists := mc.globalMetrics.StagesMetrics[parentID]; exists {
			if wm, exists := sm.WorkersMetrics[event.ID]; exists {
				wm.EndTime = event.Timestamp
				wm.Duration = wm.EndTime.Sub(wm.StartTime)
				sm.WorkersMetrics[event.ID] = wm
			}
			mc.globalMetrics.StagesMetrics[parentID] = sm
		}

		// case RouterStarted:
		// 	mc.globalMetrics.RoutersMetrics[event.ID] = RouterMetrics{
		// 		Identity:    Identity{ID: event.ID},
		// 		BaseMetrics: BaseMetrics{StartTime: event.Timestamp},
		// 		PathCounter: make(map[string]int64),
		// 		Errors:      make(map[string]error),
		// 	}
		// 	mc.getOrCreateRouterCounters(event.ID)

		// case RouterCompleted:
		// 	if rm, exists := mc.globalMetrics.RoutersMetrics[event.ID]; exists {
		// 		rm.EndTime = event.Timestamp
		// 		rm.Duration = rm.EndTime.Sub(rm.StartTime)
		// 		mc.globalMetrics.RoutersMetrics[event.ID] = rm
		// 	}

		// 	case RouterFailed:

		// case RouterPathTaken:
		// 	if rm, exists := mc.globalMetrics.RoutersMetrics[event.ID]; exists {
		// 		if pathName := getMetadataString(event.Data, "path"); pathName != "" {
		// 			rm.PathCounter[pathName]++
		// 		}
		// 		mc.globalMetrics.RoutersMetrics[event.ID] = rm
		// 	}
		// }
	}
}

// periodicAggregation aggregates local counters into global metrics
func (mc *MetricsCollector) periodicAggregation() {
	defer mc.wg.Done()

	for {
		select {
		case <-mc.aggregationTicker.C:
			mc.aggregate()
		case <-mc.ctx.Done():
			// Final aggregation
			mc.aggregate()
			return
		}
	}
}

// aggregate copies atomic counter values to global metrics
func (mc *MetricsCollector) aggregate() {
	mc.globalMu.Lock()
	defer mc.globalMu.Unlock()

	// Aggregate stage counters
	mc.stageCounters.Range(func(key, value any) bool {
		stageID := key.(string)
		counters := value.(*LocalCounters)

		if sm, exists := mc.globalMetrics.StagesMetrics[stageID]; exists {
			sm.ProcessedCounter = counters.processed.Load()
			sm.FailedCounter = counters.failed.Load()
			mc.globalMetrics.StagesMetrics[stageID] = sm
		}
		return true
	})

	// Aggregate worker counters
	mc.workerCounters.Range(func(key, value any) bool {
		workerID := key.(string)
		counters := value.(*LocalCounters)

		// Find parent stage and update worker metrics
		for stageID, sm := range mc.globalMetrics.StagesMetrics {
			if wm, exists := sm.WorkersMetrics[workerID]; exists {
				wm.ProcessedCounter = counters.processed.Load()
				wm.FailedCounter = counters.failed.Load()
				sm.WorkersMetrics[workerID] = wm
				mc.globalMetrics.StagesMetrics[stageID] = sm
				break
			}
		}
		return true
	})

	// Aggregate router counters
	mc.routerCounters.Range(func(key, value any) bool {
		routerID := key.(string)
		counters := value.(*LocalCounters)

		if rm, exists := mc.globalMetrics.RoutersMetrics[routerID]; exists {
			rm.ProcessedCounter = counters.processed.Load()
			rm.FailedCounter = counters.failed.Load()
			mc.globalMetrics.RoutersMetrics[routerID] = rm
		}
		return true
	})

	// Update global totals
	mc.globalMetrics.ProcessedCounter = mc.totalProcessed.Load()
	mc.globalMetrics.FailedCounter = mc.totalFailed.Load()
}

// Helper functions to get or create counters

func (mc *MetricsCollector) getOrCreateStageCounters(stageID string) *LocalCounters {
	if val, ok := mc.stageCounters.Load(stageID); ok {
		return val.(*LocalCounters)
	}

	counters := &LocalCounters{startTime: time.Now()}
	mc.stageCounters.Store(stageID, counters)
	return counters
}

func (mc *MetricsCollector) getOrCreateWorkerCounters(workerID string) *LocalCounters {
	if val, ok := mc.workerCounters.Load(workerID); ok {
		return val.(*LocalCounters)
	}

	counters := &LocalCounters{startTime: time.Now()}
	mc.workerCounters.Store(workerID, counters)
	return counters
}

func (mc *MetricsCollector) getOrCreateRouterCounters(routerID string) *LocalCounters {
	if val, ok := mc.routerCounters.Load(routerID); ok {
		return val.(*LocalCounters)
	}

	counters := &LocalCounters{startTime: time.Now()}
	mc.routerCounters.Store(routerID, counters)
	return counters
}

// GetMetrics returns a snapshot of current metrics
func (mc *MetricsCollector) GetMetrics() ExecutionMetrics {
	mc.globalMu.RLock()
	defer mc.globalMu.RUnlock()

	// Deep copy to avoid concurrent modification issues
	metrics := ExecutionMetrics{
		BaseMetrics: BaseMetrics{
			StartTime:        mc.globalMetrics.StartTime,
			EndTime:          time.Now(),
			ProcessedCounter: mc.totalProcessed.Load(), // Always real-time accurate
			FailedCounter:    mc.totalFailed.Load(),    // Always real-time accurate
		},
		StagesMetrics:  make(map[string]StageMetrics),
		RoutersMetrics: make(map[string]RouterMetrics),
	}
	metrics.Duration = metrics.EndTime.Sub(metrics.StartTime)

	// Copy stage metrics
	for k, v := range mc.globalMetrics.StagesMetrics {
		sm := v
		sm.WorkersMetrics = make(map[string]WorkerMetrics)
		maps.Copy(sm.WorkersMetrics, v.WorkersMetrics)
		metrics.StagesMetrics[k] = sm
	}

	// Copy router metrics
	for k, v := range mc.globalMetrics.RoutersMetrics {
		rm := v
		rm.PathCounter = make(map[string]int64)
		maps.Copy(rm.PathCounter, v.PathCounter)
		metrics.RoutersMetrics[k] = rm
	}

	return metrics
}

// GetRealtimeTotals returns always-accurate totals (no delay)
func (mc *MetricsCollector) GetRealtimeTotals() (processed, failed int64) {
	return mc.totalProcessed.Load(), mc.totalFailed.Load()
}

// Close shuts down the metrics collector gracefully
func (mc *MetricsCollector) Close() error {
	mc.cancel()
	mc.aggregationTicker.Stop()
	mc.wg.Wait()
	close(mc.structuralEvents)
	return nil
}

// SetAggregationInterval changes how often local counters are aggregated
func (mc *MetricsCollector) SetAggregationInterval(interval time.Duration) {
	mc.aggregationTicker.Reset(interval)
	mc.aggregationInterval = interval
}
