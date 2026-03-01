package gopl

import (
	"context"
	"testing"
	"time"
)

func TestMetricsCollector_OnEvent_ItemProcessed(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	mc := NewMetricsCollector(config)
	defer mc.Close()

	// First, send structural events to set up the hierarchy
	stageEvent := &Event{
		ID:       "stage1",
		Type:     StageStarted,
		Time:     time.Now(),
		Metadata: map[string]any{}, // No special metadata needed for stage start
	}
	mc.OnEvent(context.Background(), stageEvent)

	workerEvent := &Event{
		ID:       "worker1",
		Type:     WorkerStarted,
		Time:     time.Now(),
		Metadata: map[string]any{stageIDKey: "stage1"}, // Link worker to stage
	}
	mc.OnEvent(context.Background(), workerEvent)

	// Now send the item event
	itemEvent := &Event{
		ID:       "item1",
		Type:     ProcessCompleted,
		Time:     time.Now(),
		Metadata: map[string]any{stageIDKey: "stage1", workerIDKey: "worker1"}, // Link to stage and worker
	}
	mc.OnEvent(context.Background(), itemEvent)

	// Allow aggregation
	time.Sleep(300 * time.Millisecond)

	metrics := mc.GetMetrics()
	if metrics.ProcessedCounter != 1 {
		t.Errorf("Expected 1 processed, got %d", metrics.ProcessedCounter)
	}
	if sm, ok := metrics.StagesMetrics["stage1"]; !ok || sm.ProcessedCounter != 1 {
		t.Error("Expected stage counter updated")
	}
}

func TestMetricsCollector_OnEvent_ItemFailed(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	mc := NewMetricsCollector(config)
	defer mc.Close()

	event := &Event{
		ID:       "item1",
		Type:     ProcessFailed,
		Time:     time.Now(),
		Metadata: map[string]any{stageIDKey: "stage1"},
	}
	mc.OnEvent(context.Background(), event)

	time.Sleep(150 * time.Millisecond)

	metrics := mc.GetMetrics()
	if metrics.FailedCounter != 1 {
		t.Errorf("Expected 1 failed, got %d", metrics.FailedCounter)
	}
}

func TestMetricsCollector_StructuralEvents(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	mc := NewMetricsCollector(config)
	defer mc.Close()

	// Stage started
	event := &Event{
		ID:       "stage1",
		Type:     StageStarted,
		Time:     time.Now(),
		Metadata: map[string]any{},
	}
	mc.OnEvent(context.Background(), event)

	// Worker started
	event = &Event{
		ID:       "worker1",
		Type:     WorkerStarted,
		Time:     time.Now(),
		Metadata: map[string]any{stageIDKey: "stage1"},
	}
	mc.OnEvent(context.Background(), event)

	time.Sleep(150 * time.Millisecond)

	metrics := mc.GetMetrics()
	if _, ok := metrics.StagesMetrics["stage1"]; !ok {
		t.Error("Expected stage in metrics")
	}
	if sm := metrics.StagesMetrics["stage1"]; len(sm.WorkersMetrics) == 0 {
		t.Error("Expected worker in stage metrics")
	}
}

func TestMetricsCollector_GetRealtimeTotals(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	mc := NewMetricsCollector(config)
	defer mc.Close()

	event := &Event{
		ID:       "item1",
		Type:     ProcessCompleted,
		Time:     time.Now(),
		Metadata: map[string]any{},
	}
	mc.OnEvent(context.Background(), event)

	processed, failed := mc.GetRealtimeTotals()
	if processed != 1 || failed != 0 {
		t.Errorf("Expected 1 processed, 0 failed; got %d, %d", processed, failed)
	}
}

func TestMetricsCollector_Close(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	mc := NewMetricsCollector(config)

	err := mc.Close()
	if err != nil {
		t.Errorf("Expected no error on close, got %v", err)
	}
}

func TestMetricsCollector_SetAggregationInterval(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	mc := NewMetricsCollector(config)
	defer mc.Close()

	newInterval := 200 * time.Millisecond
	mc.SetAggregationInterval(newInterval)

	// Verify by checking internal state (if accessible) or just ensure no panic
	if mc.aggregationInterval != newInterval {
		t.Errorf("Expected interval %v, got %v", newInterval, mc.aggregationInterval)
	}
}
