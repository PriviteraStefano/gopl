package gopl

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// ─── PipelineHandle ───────────────────────────────────────────────────────────

// PipelineHandle is returned by PipelineBuilder.Build. It holds all live
// output channels produced by the pipeline's stages and routers, and provides
// helpers for lifecycle management and result consumption.
//
// The handle is safe to query from multiple goroutines once Build returns.
type PipelineHandle struct {
	// stageOutputs maps a component ID to its output channel (as `any`).
	// For stages: stageID → <-chan O (at runtime).
	// For router branches: "<routerID>/<branchKey>" → <-chan T (at runtime).
	stageOutputs map[string]any

	// routerOutputs maps routerID → map[branchKey]any, where each value is
	// a <-chan T for the router's element type T.
	routerOutputs map[string]map[string]any

	// done is closed when all pipeline goroutines have exited.
	done chan struct{}

	// cancel cancels the pipeline's context, signalling all stages to stop.
	cancel context.CancelFunc

	// wg tracks every running stage so that Wait / Done work correctly.
	wg *sync.WaitGroup
}

// Wait blocks until every stage in the pipeline has finished processing.
func (ph *PipelineHandle) Wait() {
	ph.wg.Wait()
}

// Cancel signals all stages to stop at their next context-check point.
// It does not wait for goroutines to exit; call Wait() afterwards for a clean
// shutdown.
func (ph *PipelineHandle) Cancel() {
	if ph.cancel != nil {
		ph.cancel()
	}
}

// Done returns a channel that is closed once all pipeline goroutines have
// finished. It can be used in a select statement as an alternative to Wait().
func (ph *PipelineHandle) Done() <-chan struct{} {
	return ph.done
}

// Results wraps the handle in a PipelineResults accessor.
func (ph *PipelineHandle) Results() *PipelineResults {
	return &PipelineResults{handle: ph}
}

// ─── PipelineResults ─────────────────────────────────────────────────────────

// PipelineResults wraps a PipelineHandle and provides typed accessor methods
// for retrieving stage and router outputs.
type PipelineResults struct {
	handle *PipelineHandle
}

// GetStageOutput returns the typed output channel for the stage identified by
// stageID. It returns an error if the stage is not found or if T does not
// match the stage's actual output type.
//
// The returned channel is closed when the stage finishes producing items.
func GetStageOutput[T any](pr *PipelineResults, stageID string) (<-chan T, error) {
	raw, ok := pr.handle.stageOutputs[stageID]
	if !ok {
		return nil, fmt.Errorf("stage %q not found in pipeline handle", stageID)
	}
	ch, ok := raw.(<-chan T)
	if !ok {
		return nil, fmt.Errorf(
			"stage %q: output channel has type %T, cannot assert to <-chan %T",
			stageID, raw, *new(T),
		)
	}
	return ch, nil
}

// GetRouterBranch returns the typed output channel for one specific branch of
// the router identified by routerID.
func GetRouterBranch[T any](pr *PipelineResults, routerID, branchKey string) (<-chan T, error) {
	branches, ok := pr.handle.routerOutputs[routerID]
	if !ok {
		return nil, fmt.Errorf("router %q not found in pipeline handle", routerID)
	}
	raw, ok := branches[branchKey]
	if !ok {
		return nil, fmt.Errorf("router %q has no branch %q", routerID, branchKey)
	}
	ch, ok := raw.(<-chan T)
	if !ok {
		return nil, fmt.Errorf(
			"router %q branch %q: channel has type %T, cannot assert to <-chan %T",
			routerID, branchKey, raw, *new(T),
		)
	}
	return ch, nil
}

// GetMergedOutput merges the output channels of multiple stages into a single
// channel. The returned channel is closed once all named stages have finished.
func GetMergedOutput[T any](pr *PipelineResults, stageIDs ...string) (<-chan T, error) {
	if len(stageIDs) == 0 {
		return nil, fmt.Errorf("no stage IDs provided to GetMergedOutput")
	}
	channels := make([]<-chan T, 0, len(stageIDs))
	for _, id := range stageIDs {
		ch, err := GetStageOutput[T](pr, id)
		if err != nil {
			return nil, err
		}
		channels = append(channels, ch)
	}
	return Merge(channels...), nil
}

// CollectAll drains the output channel of stageID into a slice and returns it.
// This call blocks until the stage has finished and its channel is closed.
func CollectAll[T any](pr *PipelineResults, stageID string) ([]T, error) {
	ch, err := GetStageOutput[T](pr, stageID)
	if err != nil {
		return nil, err
	}
	var results []T
	for item := range ch {
		results = append(results, item)
	}
	return results, nil
}

// ─── Build ────────────────────────────────────────────────────────────────────

// Build validates the DAG, wires all channels, and starts every component
// goroutine. It returns a PipelineHandle through which results can be consumed
// and the pipeline lifecycle managed.
//
// Source stages — those with no registered upstream inputs — must receive their
// input channels via the feeders map:
//
//	feeders["mySourceStage"] = (<-chan MyType)(myChan)
//
// The value must be a receive-direction channel whose element type matches the
// source stage's declared input type I.
//
// If validation fails or any component fails to start, Build returns a non-nil
// error. No goroutines are left running in that case.
func (pb *PipelineBuilder) Build(ctx context.Context, feeders map[string]any) (*PipelineHandle, error) {
	// ── 0. Validate ───────────────────────────────────────────────────────────
	vr := pb.Validate()
	if !vr.IsValid {
		return nil, fmt.Errorf("pipeline validation failed:\n%s", vr.String())
	}

	// ── 1. Cancellable context ────────────────────────────────────────────────
	buildCtx, cancel := context.WithCancel(ctx)

	// ── 2. Topological sort ───────────────────────────────────────────────────
	order, err := pb.topologicalSort()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("topological sort failed: %w", err)
	}

	// ── 3. Allocate handle ────────────────────────────────────────────────────
	handle := &PipelineHandle{
		stageOutputs:  make(map[string]any),
		routerOutputs: make(map[string]map[string]any),
		done:          make(chan struct{}),
		cancel:        cancel,
		wg:            &sync.WaitGroup{},
	}

	// ── 4. Launch components in topological order ─────────────────────────────
	for _, id := range order {
		comp := pb.components[id]

		// Each component gets its resolved config with the cancellable context
		// injected so cancellation propagates into StartWorker.
		cfg := pb.configFor(id).WithContext(buildCtx)

		switch c := comp.(type) {
		case *stageComponent:
			outCh, doneCh, launchErr := pb.launchStage(c, cfg, handle, feeders)
			if launchErr != nil {
				cancel()
				return nil, fmt.Errorf("failed to launch stage %q: %w", id, launchErr)
			}
			handle.stageOutputs[id] = outCh

			// Track this stage in the handle's WaitGroup.
			// The watcher goroutine decrements it when the stage's doneCh fires.
			handle.wg.Add(1)
			go func(d <-chan struct{}) {
				defer handle.wg.Done()
				<-d
			}(doneCh)

		case *routerComponent:
			branches, launchErr := pb.launchRouter(c, cfg, handle)
			if launchErr != nil {
				cancel()
				return nil, fmt.Errorf("failed to launch router %q: %w", id, launchErr)
			}
			handle.routerOutputs[id] = branches

			// Store each branch under its canonical branch-ref ID so that
			// downstream stages can resolve it via handle.stageOutputs.
			for branchKey, branchCh := range branches {
				refID := RouterBranchRef{RouterID: id, BranchKey: branchKey}.ID()
				handle.stageOutputs[refID] = branchCh
			}
		}
	}

	// ── 5. Close handle.done when all stages have finished ────────────────────
	go func() {
		handle.wg.Wait()
		close(handle.done)
	}()

	return handle, nil
}

// ─── launchStage ─────────────────────────────────────────────────────────────

// launchStage resolves all input channels for a stage — from already-started
// upstream outputs stored in the handle, or from the external feeders map for
// source stages — and calls the stage's launchFn.
//
// Returns:
//   - outCh:  the stage's typed output channel (as `any`, always <-chan O)
//   - doneCh: closed when the stage's worker goroutines have all exited
//   - err:    non-nil if inputs cannot be resolved or the stage fails to start
func (pb *PipelineBuilder) launchStage(
	sc *stageComponent,
	cfg *Config,
	handle *PipelineHandle,
	feeders map[string]any,
) (outCh any, doneCh <-chan struct{}, err error) {

	// Resolve input channels.
	var inputs []any

	if len(sc.inputIDs) == 0 {
		// Source stage: requires a feeder channel from the caller.
		feederCh, ok := feeders[sc.id]
		if !ok {
			return nil, nil, fmt.Errorf(
				"source stage %q has no upstream inputs and no feeder channel was provided",
				sc.id,
			)
		}
		inputs = []any{feederCh}
	} else {
		inputs = make([]any, 0, len(sc.inputIDs))
		for _, inputID := range sc.inputIDs {
			ch, ok := handle.stageOutputs[inputID]
			if !ok {
				return nil, nil, fmt.Errorf(
					"stage %q: upstream output for %q not available "+
						"(component not yet started or missing from feeders)",
					sc.id, inputID,
				)
			}
			inputs = append(inputs, ch)
		}
	}

	// Delegate to the typed closure captured at registration time.
	// It returns (outputChannel, doneChannel, error).
	return sc.launchFn(cfg, inputs)
}

// ─── launchRouter ─────────────────────────────────────────────────────────────

// launchRouter resolves the single input channel for a router from the
// handle's already-started upstream outputs and calls the router's launchFn.
func (pb *PipelineBuilder) launchRouter(
	rc *routerComponent,
	cfg *Config,
	handle *PipelineHandle,
) (map[string]any, error) {

	inputCh, ok := handle.stageOutputs[rc.inputID]
	if !ok {
		return nil, fmt.Errorf(
			"router %q: upstream output for %q not available "+
				"(component not yet started or missing)",
			rc.id, rc.inputID,
		)
	}

	return rc.launchFn(cfg, inputCh)
}

// ─── topologicalSort ─────────────────────────────────────────────────────────

// topologicalSort returns component IDs in an order such that every upstream
// component appears before all of its downstream consumers (Kahn's algorithm).
func (pb *PipelineBuilder) topologicalSort() ([]string, error) {
	// Compute in-degree for every registered component.
	inDegree := make(map[string]int, len(pb.components))
	for id := range pb.components {
		inDegree[id] = 0
	}

	for _, id := range pb.registrationOrder {
		for _, inputID := range pb.components[id].getInputIDs() {
			// Resolve router-branch references to their parent router ID.
			upstreamID := resolveComponentID(inputID)
			if _, exists := pb.components[upstreamID]; exists {
				inDegree[id]++
			}
			// If upstreamID is not a known component it is an external feeder
			// slot and does not contribute to the topological in-degree.
		}
	}

	// Seed the queue with zero-in-degree nodes, preserving registration order
	// for deterministic output.
	queue := make([]string, 0, len(pb.components))
	for _, id := range pb.registrationOrder {
		if inDegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	sorted := make([]string, 0, len(pb.components))

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		sorted = append(sorted, cur)

		// Decrement downstream components that directly reference cur.
		for _, nextID := range pb.edges[cur] {
			if _, exists := pb.components[nextID]; exists {
				inDegree[nextID]--
				if inDegree[nextID] == 0 {
					queue = append(queue, nextID)
				}
			}
		}

		// Also handle branch-ref edges keyed as "<cur>/<branchKey>" that point
		// to downstream stages referencing a specific router branch.
		if pb.components[cur].getType() == ComponentTypeRouter {
			prefix := cur + "/"
			for edgeFrom, tos := range pb.edges {
				if strings.HasPrefix(edgeFrom, prefix) {
					for _, toID := range tos {
						if _, exists := pb.components[toID]; exists {
							inDegree[toID]--
							if inDegree[toID] == 0 {
								queue = append(queue, toID)
							}
						}
					}
				}
			}
		}
	}

	if len(sorted) != len(pb.components) {
		return nil, fmt.Errorf(
			"topological sort incomplete: %d component(s) could not be ordered "+
				"(possible cycle or unresolvable dependency)",
			len(pb.components)-len(sorted),
		)
	}
	return sorted, nil
}

// ─── resolveComponentID ──────────────────────────────────────────────────────

// resolveComponentID extracts the base component ID from a string that may be
// either a plain component ID or a router branch reference ("<id>/<branch>").
func resolveComponentID(id string) string {
	if idx := strings.Index(id, "/"); idx != -1 {
		return id[:idx]
	}
	return id
}
