package gopl

import (
	"fmt"
	"strings"
)

// ─── Validation types ─────────────────────────────────────────────────────────

// ValidationError describes a single problem found during DAG validation.
type ValidationError struct {
	// Component is the ID of the offending component (may be empty for
	// graph-level errors such as cycle detection).
	Component string
	// Reason is a human-readable description of the problem.
	Reason string
}

func (e ValidationError) Error() string {
	if e.Component != "" {
		return fmt.Sprintf("component %q: %s", e.Component, e.Reason)
	}
	return e.Reason
}

// ValidationResult holds the outcome of a full DAG validation pass.
type ValidationResult struct {
	IsValid bool
	Errors  []ValidationError
}

// String returns a formatted, multi-line summary of all validation errors.
func (vr ValidationResult) String() string {
	if vr.IsValid {
		return "validation passed"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("validation failed with %d error(s):\n", len(vr.Errors)))
	for i, e := range vr.Errors {
		sb.WriteString(fmt.Sprintf("  [%d] %s\n", i+1, e.Error()))
	}
	return sb.String()
}

// ─── Validate ─────────────────────────────────────────────────────────────────

// Validate runs all DAG checks and returns a ValidationResult that accumulates
// every error found.  The checks are:
//
//  1. Registration-time errors (duplicate IDs captured during AddStage/AddRouter)
//  2. Empty graph
//  3. Worker count ≥ 1 for every stage
//  4. All referenced inputIDs exist as components or feeder slots
//  5. No cycles (DFS with 3-colour marking)
//  6. Type compatibility at every edge
//  7. No orphaned components (not reachable from any source or sink)
func (pb *PipelineBuilder) Validate() ValidationResult {
	var errs []ValidationError

	// ── 1. Registration-time errors ──────────────────────────────────────────
	for _, regErr := range pb.registrationErrors {
		errs = append(errs, ValidationError{Reason: regErr.Error()})
	}

	// ── 2. Empty graph ───────────────────────────────────────────────────────
	if len(pb.components) == 0 {
		errs = append(errs, ValidationError{Reason: "pipeline has no components"})
		return ValidationResult{IsValid: false, Errors: errs}
	}

	// ── 3. Worker counts ─────────────────────────────────────────────────────
	for _, id := range pb.registrationOrder {
		c := pb.components[id]
		if c.getType() == ComponentTypeStage {
			sc := c.(*stageComponent)
			if sc.workers < 1 {
				errs = append(errs, ValidationError{
					Component: id,
					Reason:    fmt.Sprintf("workers must be ≥ 1, got %d", sc.workers),
				})
			}
		}
	}

	// ── 4. All referenced inputIDs resolve ───────────────────────────────────
	//
	// A valid inputID is one of:
	//   a) Another registered component ID, OR
	//   b) A RouterBranchRef canonical ID ("<routerID>/<branchKey>") where
	//      routerID is a registered router component.
	//
	// An empty inputIDs slice is valid — the stage is a source node and will
	// be fed by an external feeder channel at Start time.
	for _, id := range pb.registrationOrder {
		c := pb.components[id]
		for _, inputID := range c.getInputIDs() {
			if !pb.inputIDExists(inputID) {
				errs = append(errs, ValidationError{
					Component: id,
					Reason:    fmt.Sprintf("references unknown inputID %q", inputID),
				})
			}
		}
	}

	// ── 5. Cycle detection ───────────────────────────────────────────────────
	if cycles := pb.detectCycles(); len(cycles) > 0 {
		for _, cycle := range cycles {
			errs = append(errs, ValidationError{
				Reason: fmt.Sprintf("cycle detected: %s", strings.Join(cycle, " → ")),
			})
		}
	}

	// ── 6. Type compatibility at every edge ──────────────────────────────────
	typeErrs := pb.checkTypeCompatibility()
	errs = append(errs, typeErrs...)

	// ── 7. Orphaned components ───────────────────────────────────────────────
	orphanErrs := pb.checkOrphans()
	errs = append(errs, orphanErrs...)

	return ValidationResult{IsValid: len(errs) == 0, Errors: errs}
}

// ─── inputIDExists ────────────────────────────────────────────────────────────

// inputIDExists returns true when inputID refers to either a known component
// or a valid router-branch reference ("<routerID>/<branchKey>").
func (pb *PipelineBuilder) inputIDExists(inputID string) bool {
	// Direct component reference.
	if _, ok := pb.components[inputID]; ok {
		return true
	}
	// Router branch reference: "<routerID>/<branchKey>".
	if idx := strings.Index(inputID, "/"); idx != -1 {
		routerID := inputID[:idx]
		if c, ok := pb.components[routerID]; ok {
			return c.getType() == ComponentTypeRouter
		}
	}
	return false
}

// ─── Cycle detection (3-colour DFS) ──────────────────────────────────────────

// nodeColour represents the DFS traversal state of a node.
type nodeColour int

const (
	colourWhite nodeColour = iota // not yet visited
	colourGrey                    // currently on the DFS stack
	colourBlack                   // fully processed
)

// detectCycles performs a depth-first search over the component graph and
// returns all distinct cycles as slices of component IDs.
func (pb *PipelineBuilder) detectCycles() [][]string {
	colour := make(map[string]nodeColour, len(pb.components))
	parent := make(map[string]string, len(pb.components))
	var cycles [][]string

	var dfs func(id string)
	dfs = func(id string) {
		colour[id] = colourGrey
		for _, neighbour := range pb.edges[id] {
			switch colour[neighbour] {
			case colourGrey:
				// Back-edge found → cycle.
				cycle := pb.extractCycle(parent, id, neighbour)
				cycles = append(cycles, cycle)
			case colourWhite:
				parent[neighbour] = id
				dfs(neighbour)
			}
			// colourBlack: already fully explored, skip.
		}
		colour[id] = colourBlack
	}

	// Initialise all known nodes to white, including virtual branch ref nodes.
	for id := range pb.components {
		colour[id] = colourWhite
	}
	// Also seed virtual branch-ref keys that appear in edge lists.
	for from := range pb.edges {
		if _, ok := colour[from]; !ok {
			colour[from] = colourWhite
		}
	}

	// Run DFS from every unvisited node so disconnected subgraphs are covered.
	for id := range pb.components {
		if colour[id] == colourWhite {
			dfs(id)
		}
	}

	return cycles
}

// extractCycle reconstructs the cycle path from the parent map.
// entryPoint is where the back-edge lands (the grey node), backEdgeFrom is
// the node that has the back-edge.
func (pb *PipelineBuilder) extractCycle(parent map[string]string, backEdgeFrom, entryPoint string) []string {
	var cycle []string
	cycle = append(cycle, entryPoint)

	cur := backEdgeFrom
	for cur != entryPoint && cur != "" {
		cycle = append([]string{cur}, cycle...)
		cur = parent[cur]
	}
	// Close the cycle visually.
	cycle = append(cycle, entryPoint)
	return cycle
}

// ─── Type compatibility ───────────────────────────────────────────────────────

// checkTypeCompatibility verifies that the output type of every upstream
// component matches the input type of every downstream component across all
// edges in the graph.
func (pb *PipelineBuilder) checkTypeCompatibility() []ValidationError {
	var errs []ValidationError

	for fromID, toIDs := range pb.edges {
		fromType, ok := pb.resolvedOutputType(fromID)
		if !ok {
			// fromID is a feeder slot (external) — no type info to check.
			continue
		}

		for _, toID := range toIDs {
			// toID may be a real component or a virtual branch-ref node
			// (which has no entry in pb.components).
			toComp, exists := pb.components[toID]
			if !exists {
				continue
			}
			toInputType := toComp.getInputType()
			if toInputType == nil {
				continue
			}
			if fromType != toInputType {
				errs = append(errs, ValidationError{
					Component: toID,
					Reason: fmt.Sprintf(
						"type mismatch: upstream %q outputs %s, but this component expects %s",
						fromID, fromType, toInputType,
					),
				})
			}
		}
	}
	return errs
}

// resolvedOutputType returns the output reflect.Type for a component ID,
// handling both plain component IDs and router-branch references
// ("<routerID>/<branchKey>").  The second return value is false when the ID
// is unknown (e.g. an external feeder slot).
func (pb *PipelineBuilder) resolvedOutputType(id string) (interface{ String() string }, bool) {
	// Plain component.
	if c, ok := pb.components[id]; ok {
		return c.getOutputType(), true
	}
	// Router branch reference.
	if idx := strings.Index(id, "/"); idx != -1 {
		routerID := id[:idx]
		if c, ok := pb.components[routerID]; ok && c.getType() == ComponentTypeRouter {
			return c.getOutputType(), true
		}
	}
	return nil, false
}

// ─── Orphan detection ─────────────────────────────────────────────────────────

// checkOrphans detects components that are neither reachable from any source
// node (a stage with no inputs) nor contributing to any sink node (a stage
// with no downstream consumers).
//
// The algorithm:
//  1. Forward BFS from all source nodes → reachableFromSource
//  2. Reverse BFS from all sink nodes   → reachableFromSink
//  3. Any component absent from both sets is orphaned.
func (pb *PipelineBuilder) checkOrphans() []ValidationError {
	if len(pb.components) == 0 {
		return nil
	}

	// Build the reverse adjacency list.
	reverse := make(map[string][]string, len(pb.components))
	for from, tos := range pb.edges {
		for _, to := range tos {
			reverse[to] = append(reverse[to], from)
		}
	}

	// Identify source nodes (stages/routers with no upstream component inputs).
	// A component that only references external feeders (inputIDs that are not
	// registered components) counts as a source.
	isSource := func(c Component) bool {
		for _, inputID := range c.getInputIDs() {
			if pb.inputIDExists(inputID) {
				// At least one upstream component exists → not a pure source.
				// (RouterBranchRef IDs are also valid component references here.)
				if _, ok := pb.components[inputID]; ok {
					return false
				}
				// It's a branch ref — the router is a registered component so
				// this stage has an upstream node.
				if idx := strings.Index(inputID, "/"); idx != -1 {
					return false
				}
			}
		}
		return true
	}

	// Identify sink nodes (components with no registered downstream consumers).
	isSink := func(id string) bool {
		return len(pb.edges[id]) == 0
	}

	// Forward BFS from sources.
	reachableFromSource := pb.bfsForward(isSource)

	// Reverse BFS from sinks.
	reachableFromSink := pb.bfsReverse(isSink, reverse)

	var errs []ValidationError
	for _, id := range pb.registrationOrder {
		inFwd := reachableFromSource[id]
		inRev := reachableFromSink[id]
		if !inFwd && !inRev {
			errs = append(errs, ValidationError{
				Component: id,
				Reason:    "orphaned component: not reachable from any source or sink",
			})
		}
	}
	return errs
}

// bfsForward performs a BFS over pb.edges starting from all nodes where
// seedFn returns true, and returns the set of reachable node IDs.
func (pb *PipelineBuilder) bfsForward(seedFn func(Component) bool) map[string]bool {
	visited := make(map[string]bool, len(pb.components))
	queue := make([]string, 0, len(pb.components))

	for id, c := range pb.components {
		if seedFn(c) {
			visited[id] = true
			queue = append(queue, id)
		}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range pb.edges[cur] {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}
	return visited
}

// bfsReverse performs a BFS over the reversed graph starting from all nodes
// where sinkFn returns true, and returns the set of reachable node IDs.
func (pb *PipelineBuilder) bfsReverse(sinkFn func(string) bool, reverse map[string][]string) map[string]bool {
	visited := make(map[string]bool, len(pb.components))
	queue := make([]string, 0, len(pb.components))

	for id := range pb.components {
		if sinkFn(id) {
			visited[id] = true
			queue = append(queue, id)
		}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, prev := range reverse[cur] {
			// prev may be a branch-ref virtual node; resolve to the actual
			// router component before marking it visited.
			resolved := prev
			if idx := strings.Index(prev, "/"); idx != -1 {
				resolved = prev[:idx]
			}
			if !visited[resolved] {
				visited[resolved] = true
				queue = append(queue, resolved)
			}
		}
	}
	return visited
}
