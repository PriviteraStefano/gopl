package gopl

import (
	"fmt"
	"reflect"
	"sync"
)

// ─── Component Types ──────────────────────────────────────────────────────────

// ComponentType identifies whether a registered component is a stage or a router.
type ComponentType string

const (
	ComponentTypeStage  ComponentType = "stage"
	ComponentTypeRouter ComponentType = "router"
)

// Component is the common interface implemented by every node in the DAG.
type Component interface {
	getID() string
	getType() ComponentType
	getInputType() reflect.Type
	getOutputType() reflect.Type
	getInputIDs() []string
}

// ─── stageComponent ──────────────────────────────────────────────────────────

// stageComponent is the internal representation of a stage node in the DAG.
//
// launchFn is a typed closure captured at registration time. It starts the
// stage's goroutines and returns:
//   - outCh:  the stage's output channel, typed as `any` (always <-chan O at runtime)
//   - doneCh: closed when all of the stage's worker goroutines have exited
//   - err:    non-nil if the stage could not be started
//
// Separating the done signal from the output channel lets the PipelineHandle
// track goroutine lifecycle without consuming (and therefore stealing) items
// from the output channel.
type stageComponent struct {
	id         string
	workers    int
	inputIDs   []string
	inputType  reflect.Type
	outputType reflect.Type

	// launchFn starts the stage and returns (outputChannel, doneChannel, error).
	// inputs is a slice of upstream <-chan I channels, each stored as `any`.
	launchFn func(cfg *Config, inputs []any) (outCh any, doneCh <-chan struct{}, err error)
}

func (s *stageComponent) getID() string               { return s.id }
func (s *stageComponent) getType() ComponentType      { return ComponentTypeStage }
func (s *stageComponent) getInputType() reflect.Type  { return s.inputType }
func (s *stageComponent) getOutputType() reflect.Type { return s.outputType }
func (s *stageComponent) getInputIDs() []string       { return s.inputIDs }

// ─── routerComponent ─────────────────────────────────────────────────────────

// routerComponent is the internal representation of a router node in the DAG.
// A router fans one input channel out to multiple named output channels.
type routerComponent struct {
	id         string
	inputID    string
	inputType  reflect.Type
	outputType reflect.Type // same as inputType for routers

	// launchFn starts the router and returns map[branchKey]any where each
	// value is a <-chan T for that branch.
	launchFn func(cfg *Config, input any) (map[string]any, error)
}

func (r *routerComponent) getID() string               { return r.id }
func (r *routerComponent) getType() ComponentType      { return ComponentTypeRouter }
func (r *routerComponent) getInputType() reflect.Type  { return r.inputType }
func (r *routerComponent) getOutputType() reflect.Type { return r.outputType }
func (r *routerComponent) getInputIDs() []string       { return []string{r.inputID} }

// ─── RouterBranchRef ─────────────────────────────────────────────────────────

// RouterBranchRef identifies a specific output branch of a router.
//
// When a downstream stage wants to consume only one branch of a router's
// output, pass RouterBranchRef{RouterID: "myRouter", BranchKey: "premium"}.ID()
// as the inputID for that stage. The canonical form is "<routerID>/<branchKey>".
type RouterBranchRef struct {
	RouterID  string
	BranchKey string
}

// ID returns the canonical string identifier for this branch reference.
func (r RouterBranchRef) ID() string {
	return r.RouterID + "/" + r.BranchKey
}

// ─── PipelineBuilder ─────────────────────────────────────────────────────────

// PipelineBuilder constructs a DAG of stages and routers, validates it, and
// orchestrates channel wiring and goroutine lifecycle when built.
//
// Because Go generics cannot be attached to methods, stage and router
// registrations use package-level generic functions (AddStage, AddRouter) that
// accept the builder as their first argument:
//
//	pb := pipeline.NewPipelineBuilder(cfg)
//	pipeline.AddStage(pb, "parse",   2, parseFn)
//	pipeline.AddStage(pb, "enrich",  4, enrichFn, "parse")
//	pipeline.AddRouter(pb, "route",  routeFn,     "enrich")
//	pipeline.AddStage(pb, "persist", 2, persistFn,
//	    pipeline.RouterBranchRef{RouterID: "route", BranchKey: "premium"}.ID())
//
//	handle, err := pb.Build(ctx, nil)
type PipelineBuilder struct {
	defaultCfg       *Config
	componentConfigs map[string]*Config

	// components holds every registered node (stage or router), keyed by ID.
	components map[string]Component

	// edges is an adjacency list: edges[fromID] = []toID.
	// An edge means "fromID's output feeds toID's input".
	edges map[string][]string

	// registrationOrder preserves insertion order for deterministic output.
	registrationOrder []string

	// registrationErrors accumulates errors found at registration time so that
	// Build() can report them all together.
	registrationErrors []error
}

// NewPipelineBuilder creates an empty PipelineBuilder with the given default
// Config. If cfg is nil, DefaultConfig() is used.
func NewPipelineBuilder(cfg *Config) *PipelineBuilder {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &PipelineBuilder{
		defaultCfg:       cfg,
		componentConfigs: make(map[string]*Config),
		components:       make(map[string]Component),
		edges:            make(map[string][]string),
	}
}

// ─── Internal registration ───────────────────────────────────────────────────

func (pb *PipelineBuilder) registerComponent(c Component) error {
	id := c.getID()
	if id == "" {
		return fmt.Errorf("component id must not be empty")
	}
	if _, exists := pb.components[id]; exists {
		return fmt.Errorf("component %q already registered", id)
	}
	pb.components[id] = c
	pb.registrationOrder = append(pb.registrationOrder, id)

	// Wire adjacency: for every declared upstream, record fromID → thisID.
	for _, inputID := range c.getInputIDs() {
		pb.edges[inputID] = append(pb.edges[inputID], id)
	}
	return nil
}

func (pb *PipelineBuilder) recordError(err error) {
	pb.registrationErrors = append(pb.registrationErrors, err)
}

// ─── AddStage ────────────────────────────────────────────────────────────────

// AddStage registers a new typed stage in the DAG.
//
//   - pb:       the builder to register against
//   - id:       unique name for this stage
//   - workers:  number of concurrent worker goroutines (must be ≥ 1)
//   - fn:       the processing function (I → O)
//   - inputIDs: upstream component IDs whose output channels feed this stage.
//     Use RouterBranchRef.ID() to target a specific router branch.
//     Omit entirely for source stages that are fed via Build's feeders map.
//
// Returns pb so call sites can chain further Add* calls.
func AddStage[I Identifier, O Identifier](
	pb *PipelineBuilder,
	id string,
	workers int,
	fn func(I) (O, error),
	inputIDs ...string,
) *PipelineBuilder {
	sc := &stageComponent{
		id:         id,
		workers:    workers,
		inputIDs:   inputIDs,
		inputType:  reflect.TypeFor[I](),
		outputType: reflect.TypeFor[O](),
		launchFn: func(cfg *Config, inputs []any) (any, <-chan struct{}, error) {
			// Convert []any → []<-chan I
			channels := make([]<-chan I, len(inputs))
			for i, raw := range inputs {
				ch, ok := raw.(<-chan I)
				if !ok {
					return nil, nil, fmt.Errorf(
						"stage %q: input %d has wrong channel type %T, want <-chan %s",
						id, i, raw, reflect.TypeFor[I](),
					)
				}
				channels[i] = ch
			}
			return startStageWithDone(cfg, id, workers, fn, channels)
		},
	}

	if err := pb.registerComponent(sc); err != nil {
		pb.recordError(err)
	}
	return pb
}

// AddStageAfter is a convenience wrapper around AddStage for the common case
// of a single upstream dependency.
func AddStageAfter[I Identifier, O Identifier](
	pb *PipelineBuilder,
	id string,
	workers int,
	fn func(I) (O, error),
	afterID string,
) *PipelineBuilder {
	return AddStage(pb, id, workers, fn, afterID)
}

// ─── AddRouter ───────────────────────────────────────────────────────────────

// AddRouter registers a router node in the DAG.
//
//   - pb:      the builder to register against
//   - id:      unique name for this router
//   - fn:      routing function: receives the upstream channel and returns a
//     map of branch-key → typed output channel
//   - inputID: the single upstream component ID that feeds this router
//
// Downstream stages that consume a specific branch must use
// RouterBranchRef{RouterID: id, BranchKey: key}.ID() as their inputID.
func AddRouter[T any](
	pb *PipelineBuilder,
	id string,
	fn func(<-chan T) map[string]<-chan T,
	inputID string,
) *PipelineBuilder {
	rc := &routerComponent{
		id:         id,
		inputID:    inputID,
		inputType:  reflect.TypeFor[T](),
		outputType: reflect.TypeFor[T](),
		launchFn: func(cfg *Config, input any) (map[string]any, error) {
			ch, ok := input.(<-chan T)
			if !ok {
				return nil, fmt.Errorf(
					"router %q: input has wrong channel type %T, want <-chan %s",
					id, input, reflect.TypeFor[T](),
				)
			}
			cfg.EmitEvent(NewEventRouteStarted(id, inputID, nil))
			branches := fn(ch)
			result := make(map[string]any, len(branches))
			for k, v := range branches {
				result[k] = v
			}
			cfg.EmitEvent(NewEventRouteCompleted(id, id, nil))
			return result, nil
		},
	}

	if err := pb.registerComponent(rc); err != nil {
		pb.recordError(err)
	}
	return pb
}

// AddRouterAfter is syntactic sugar for AddRouter that emphasises the upstream
// dependency in the call signature.
func AddRouterAfter[T any](
	pb *PipelineBuilder,
	id string,
	fn func(<-chan T) map[string]<-chan T,
	afterID string,
) *PipelineBuilder {
	return AddRouter(pb, id, fn, afterID)
}

// ─── Config overrides ────────────────────────────────────────────────────────

// WithDefaultConfig replaces the default Config used for all components that
// have no per-component override. Returns the builder for chaining.
func (pb *PipelineBuilder) WithDefaultConfig(cfg *Config) *PipelineBuilder {
	pb.defaultCfg = cfg
	return pb
}

// WithComponentConfig sets a per-component Config override for componentID.
// Returns the builder for chaining.
func (pb *PipelineBuilder) WithComponentConfig(componentID string, cfg *Config) *PipelineBuilder {
	pb.componentConfigs[componentID] = cfg
	return pb
}

// WithComponentConfigs applies the same Config override to multiple components.
// Returns the builder for chaining.
func (pb *PipelineBuilder) WithComponentConfigs(cfg *Config, componentIDs ...string) *PipelineBuilder {
	for _, id := range componentIDs {
		pb.componentConfigs[id] = cfg
	}
	return pb
}

// configFor returns the effective Config for a component, applying the
// resolution hierarchy: per-component > builder default > DefaultConfig().
func (pb *PipelineBuilder) configFor(componentID string) *Config {
	if cfg, ok := pb.componentConfigs[componentID]; ok && cfg != nil {
		return cfg
	}
	if pb.defaultCfg != nil {
		return pb.defaultCfg
	}
	return DefaultConfig()
}

// ─── Introspection ───────────────────────────────────────────────────────────

// Components returns a snapshot of all registered components keyed by ID.
func (pb *PipelineBuilder) Components() map[string]Component {
	snapshot := make(map[string]Component, len(pb.components))
	for k, v := range pb.components {
		snapshot[k] = v
	}
	return snapshot
}

// RegistrationOrder returns a copy of component IDs in registration order.
func (pb *PipelineBuilder) RegistrationOrder() []string {
	cp := make([]string, len(pb.registrationOrder))
	copy(cp, pb.registrationOrder)
	return cp
}

// Edges returns a snapshot of the adjacency list (fromID → []toID).
func (pb *PipelineBuilder) Edges() map[string][]string {
	snapshot := make(map[string][]string, len(pb.edges))
	for k, v := range pb.edges {
		cp := make([]string, len(v))
		copy(cp, v)
		snapshot[k] = cp
	}
	return snapshot
}

// ─── startStageWithDone ──────────────────────────────────────────────────────

// startStageWithDone is the core launch helper used by every stageComponent's
// launchFn. It mirrors StartStage from stage.go but additionally returns a
// doneCh that is closed once all worker goroutines have exited.
//
// This allows the PipelineHandle to track goroutine lifecycle without
// consuming items from the output channel (which belongs to the downstream
// consumer or result reader).
func startStageWithDone[I Identifier, O Identifier](
	cfg *Config,
	id string,
	workers int,
	fn func(I) (O, error),
	inputs []<-chan I,
) (outCh any, doneCh <-chan struct{}, err error) {
	if len(inputs) == 0 {
		return nil, nil, fmt.Errorf("stage %q: no input channels provided", id)
	}

	var ch <-chan I
	if len(inputs) == 1 {
		ch = inputs[0]
	} else {
		ch = Merge(inputs...)
	}

	cfg.EmitEvent(NewEventStageStarted(id, workers, nil))

	out := make(chan O, workers)
	done := make(chan struct{})

	var stageWg sync.WaitGroup
	stageWg.Add(workers)

	for workerID := 0; workerID < workers; workerID++ {
		go StartWorker(WorkerConfig[I, O]{
			StageConfig: StageConfig[I, O]{
				cfg:      cfg,
				StageId:  id,
				Workers:  workers,
				fn:       fn,
				channels: []<-chan I{ch},
			},
			WorkerId: workerID,
			StageWg:  &stageWg,
			Out:      out,
		})
	}

	go func() {
		stageWg.Wait()
		close(out)
		cfg.EmitEvent(NewEventStageCompleted(id, nil))
		close(done)
	}()

	return (<-chan O)(out), done, nil
}
