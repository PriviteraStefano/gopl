package gopl

import (
	"maps"
	"time"
)

type Identifier interface {
	GetID() string
}

type EventType string

const (
	// Stage events
	StageStarted     EventType = "stage_started"
	StageCompleted   EventType = "stage_finished"
	WorkerStarted    EventType = "worker_started"
	WorkerCompleted  EventType = "worker_finished"
	ProcessStarted   EventType = "item_process_started"
	ProcessCompleted EventType = "item_processed"
	ProcessFailed    EventType = "item_failed"

	// Router events
	RouterStarted   EventType = "router_started"
	RouterCompleted EventType = "router_finished"
	RouterFailed    EventType = "router_error"

	// Basic events
	DebugEventType   EventType = "debug"
	WarningEventType EventType = "warning"
	ErrorEventType   EventType = "error"
)

type Eventful interface {
	GetID() string
	GetType() EventType
	GetTime() time.Time
	GetMetadata() map[string]any
}

type Event struct {
	ID       string
	Type     EventType
	Time     time.Time
	Metadata map[string]any
}

func (e *Event) GetID() string               { return e.ID }
func (e *Event) GetType() EventType          { return e.Type }
func (e *Event) GetTime() time.Time          { return e.Time }
func (e *Event) GetMetadata() map[string]any { return e.Metadata }

// type StarterEvent Event
// type CompleterEvent Event
type ErrorEvent struct {
	Event
	Error error
}

type WarningEvent struct {
	Event
	Warning string
}

type DebugEvent struct {
	Event
	Message string
}

// Metadata keys
const (
	inputChannelKey  string = "input-channel"
	outputChannelKey string = "output-channel"
	workersKey       string = "workers"
	stageIDKey       string = "stage-id"
	workerIDKey      string = "worker-id"
)

// Helper function to create metadata with extras
func createMetadata(extras map[string]any, additionalCapacity int) map[string]any {
	if extras != nil {
		metadata := make(map[string]any, len(extras)+additionalCapacity)
		maps.Copy(metadata, extras)
		return metadata
	}
	return make(map[string]any, additionalCapacity)
}

// ROUTE EVENTS
func NewEventRouteStarted(id string, inputChannel string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 1)
	metadata[inputChannelKey] = inputChannel

	return &Event{id, RouterStarted, time.Now(), metadata}
}

func NewEventRouteCompleted(id string, outputChannel string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 1)
	metadata[outputChannelKey] = outputChannel

	return &Event{id, RouterCompleted, time.Now(), metadata}
}

func NewEventRouteFailed(id string, err error, extras map[string]any) *ErrorEvent {
	metadata := createMetadata(extras, 0)

	return &ErrorEvent{
		Event{id, RouterFailed, time.Now(), metadata},
		err,
	}
}

// STAGE EVENTS
func NewEventStageStarted(id string, workers int, extras map[string]any) *Event {
	metadata := createMetadata(extras, 1)
	metadata[workersKey] = workers

	return &Event{id, StageStarted, time.Now(), metadata}
}

func NewEventStageCompleted(id string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 0)

	return &Event{id, StageCompleted, time.Now(), metadata}
}

// WORKER EVENTS
func NewEventWorkerStarted(id string, stageID string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 1)
	metadata[stageIDKey] = stageID

	return &Event{id, WorkerStarted, time.Now(), metadata}
}

func NewEventWorkerCompleted(id string, stageID string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 1)
	metadata[stageIDKey] = stageID

	return &Event{id, WorkerCompleted, time.Now(), metadata}
}

// ITEM EVENTS
func NewEventProcessStarted(id string, stageID string, workerID string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 2)
	metadata[stageIDKey] = stageID
	metadata[workerIDKey] = workerID

	return &Event{id, ProcessStarted, time.Now(), metadata}
}

func NewEventProcessCompleted(id string, stageID string, workerID string, extras map[string]any) *Event {
	metadata := createMetadata(extras, 2)
	metadata[stageIDKey] = stageID
	metadata[workerIDKey] = workerID

	return &Event{id, ProcessCompleted, time.Now(), metadata}
}

func NewEventProcessFailed(id string, stageID string, workerID string, err error, extras map[string]any) *ErrorEvent {
	metadata := createMetadata(extras, 2)
	metadata[stageIDKey] = stageID
	metadata[workerIDKey] = workerID

	return &ErrorEvent{
		Event{id, ProcessFailed, time.Now(), metadata},
		err,
	}
}
