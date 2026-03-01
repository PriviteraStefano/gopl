package gopl

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// StartStageWithErr starts a stage with error handling using a raw error channel.
// Items that fail processing are sent to ec; successful results are forwarded to
// the returned channel.
func StartStageWithErr[I, O any](
	id string,
	workers int,
	fn func(I) (O, error),
	ec chan error, c ...<-chan I,
) <-chan O {
	if len(c) == 0 {
		ec <- fmt.Errorf("no input channels provided")
		return nil
	}
	var ch <-chan I
	if len(c) == 1 {
		ch = c[0]
	} else {
		ch = Merge(c...)
	}

	out := make(chan O, workers)
	var stageWg sync.WaitGroup
	stageWg.Add(workers)

	for range workers {
		go func() {
			defer stageWg.Done()
			for item := range ch {
				result, err := fn(item)
				if err != nil {
					ec <- fmt.Errorf("%s stage processing error | %w", id, err)
					continue
				}
				out <- result
			}
		}()
	}

	go func() {
		stageWg.Wait()
		close(out)
	}()

	return out
}

// StageConfig holds all configuration and state needed to run a single stage.
type StageConfig[I Identifier, O Identifier] struct {
	cfg      *Config
	Workers  int
	StageId  string
	fn       func(I) (O, error)
	channels []<-chan I
}

// NewStageConfig creates a new StageConfig with the given parameters.
func NewStageConfig[I Identifier, O Identifier](
	cfg *Config,
	stageId string,
	workers int,
	fn func(I) (O, error),
	channels ...<-chan I,
) StageConfig[I, O] {
	return StageConfig[I, O]{
		cfg:      cfg,
		StageId:  stageId,
		Workers:  workers,
		fn:       fn,
		channels: channels,
	}
}

// Start launches the stage and returns its output channel.
func (sc *StageConfig[I, O]) Start() (<-chan O, error) {
	if len(sc.channels) == 0 {
		return nil, fmt.Errorf("no input channels provided")
	}

	sc.cfg.EmitEvent(NewEventStageStarted(sc.StageId, sc.Workers, nil))
	out := make(chan O, sc.Workers)
	var stageWg sync.WaitGroup
	stageWg.Add(sc.Workers)

	for workerId := 0; workerId < sc.Workers; workerId++ {
		go StartWorker(WorkerConfig[I, O]{
			StageConfig: *sc,
			WorkerId:    workerId,
			StageWg:     &stageWg,
			Out:         out,
		})
	}

	go func() {
		stageWg.Wait()
		close(out)
		sc.cfg.EmitEvent(NewEventStageCompleted(sc.StageId, nil))
	}()

	return out, nil
}

// StartStage is the primary convenience entry-point. It accepts the stage
// parameters directly and starts the stage, returning the output channel.
//
// Both I and O must satisfy Identifier.
//
// Example:
//
//	out, err := StartStage(cfg, "enrich", 2, enrichFn, inputCh)
func StartStage[I Identifier, O Identifier](
	cfg *Config,
	stageId string,
	workers int,
	fn func(I) (O, error),
	channels ...<-chan I,
) (<-chan O, error) {
	sc := NewStageConfig(cfg, stageId, workers, fn, channels...)
	return StartStageConfig(sc)
}

// StartStageConfig starts a stage from a pre-built StageConfig value.
// This is useful when you construct the config separately (e.g. via
// NewStageConfig or StageBuilder) before launching.
func StartStageConfig[I Identifier, O Identifier](sc StageConfig[I, O]) (<-chan O, error) {
	if len(sc.channels) == 0 {
		return nil, fmt.Errorf("no input channels provided")
	}

	sc.cfg.EmitEvent(NewEventStageStarted(sc.StageId, sc.Workers, nil))

	out := make(chan O, sc.Workers)
	var stageWg sync.WaitGroup
	stageWg.Add(sc.Workers)

	for workerId := 0; workerId < sc.Workers; workerId++ {
		go StartWorker(WorkerConfig[I, O]{
			StageConfig: sc,
			WorkerId:    workerId,
			StageWg:     &stageWg,
			Out:         out,
		})
	}

	go func() {
		stageWg.Wait()
		close(out)
		sc.cfg.EmitEvent(NewEventStageCompleted(sc.StageId, nil))
	}()

	return out, nil
}

// WorkerConfig bundles everything a single worker goroutine needs.
type WorkerConfig[I Identifier, O Identifier] struct {
	StageConfig[I, O]
	WorkerId int
	StageWg  *sync.WaitGroup
	Out      chan O
}

// StartWorker runs a single worker goroutine for a stage.
func StartWorker[I Identifier, O Identifier](wc WorkerConfig[I, O]) {
	var ch <-chan I
	if len(wc.channels) == 1 {
		ch = wc.channels[0]
	} else {
		ch = Merge(wc.channels...)
	}
	wc.cfg.EmitEvent(NewEventWorkerStarted(strconv.Itoa(wc.WorkerId), wc.StageId, nil))
	defer wc.StageWg.Done()
	select {
	case <-wc.cfg.ctx().Done():
		return
	default:
		for item := range ch {
			StartProcess(ProcessConfig[I, O]{WorkerConfig: wc, Item: item})
		}
		wc.cfg.EmitEvent(NewEventWorkerCompleted(strconv.Itoa(wc.WorkerId), wc.StageId, nil))
	}
}

// ProcessConfig bundles everything needed to process a single item.
type ProcessConfig[I Identifier, O Identifier] struct {
	WorkerConfig[I, O]
	Item I
}

// StartProcess processes a single item through the stage function.
func StartProcess[I Identifier, O Identifier](pc ProcessConfig[I, O]) {
	pc.cfg.EmitEvent(NewEventProcessStarted(pc.Item.GetID(), strconv.Itoa(pc.WorkerId), pc.StageId, nil))
	result, err := pc.fn(pc.Item)
	if err != nil {
		pc.cfg.EmitEvent(NewEventProcessFailed(pc.Item.GetID(), strconv.Itoa(pc.WorkerId), pc.StageId, err, nil))
		return
	}
	pc.cfg.EmitEvent(NewEventProcessCompleted(pc.Item.GetID(), strconv.Itoa(pc.WorkerId), pc.StageId, nil))
	pc.Out <- result
}

// ─── StageBuilder ────────────────────────────────────────────────────────────

// StageBuilder provides a fluent API for constructing a StageConfig.
type StageBuilder[I Identifier, O Identifier] struct {
	cfg      *Config
	workers  int
	stageId  string
	fn       func(I) (O, error)
	channels []<-chan I
}

// NewStageBuilder returns an empty StageBuilder.
func NewStageBuilder[I Identifier, O Identifier]() *StageBuilder[I, O] {
	return &StageBuilder[I, O]{}
}

func (b *StageBuilder[I, O]) WithConfig(cfg *Config) *StageBuilder[I, O] {
	b.cfg = cfg
	return b
}

func (b *StageBuilder[I, O]) WithChannels(channels ...<-chan I) *StageBuilder[I, O] {
	b.channels = channels
	return b
}

func (b *StageBuilder[I, O]) WithStageId(stageId string) *StageBuilder[I, O] {
	b.stageId = stageId
	return b
}

func (b *StageBuilder[I, O]) WithWorkers(workers int) *StageBuilder[I, O] {
	b.workers = workers
	return b
}

func (b *StageBuilder[I, O]) WithFunction(fn func(I) (O, error)) *StageBuilder[I, O] {
	b.fn = fn
	return b
}

func (b *StageBuilder[I, O]) AddChannel(channel <-chan I) *StageBuilder[I, O] {
	b.channels = append(b.channels, channel)
	return b
}

func (b *StageBuilder[I, O]) AddChannels(channels ...<-chan I) *StageBuilder[I, O] {
	b.channels = append(b.channels, channels...)
	return b
}

// Build validates all required fields and returns a StageConfig ready to start.
func (b *StageBuilder[I, O]) Build() (*StageConfig[I, O], error) {
	if b.cfg == nil {
		return nil, errors.New("config not set")
	}
	if b.stageId == "" {
		return nil, errors.New("no stage id")
	}
	if b.workers == 0 {
		return nil, errors.New("no workers")
	}
	if b.fn == nil {
		return nil, errors.New("no function")
	}
	if len(b.channels) == 0 {
		return nil, errors.New("no channels")
	}
	return &StageConfig[I, O]{
		cfg:      b.cfg,
		StageId:  b.stageId,
		Workers:  b.workers,
		fn:       b.fn,
		channels: b.channels,
	}, nil
}
