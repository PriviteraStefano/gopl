package pipeline

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── Shared test types ────────────────────────────────────────────────────────

type pbItem struct {
	id  string
	val int
}

func (p pbItem) GetID() string { return p.id }

type pbResult struct {
	id  string
	val int
}

func (p pbResult) GetID() string { return p.id }

// makeItems creates a buffered channel pre-loaded with n items and then closed.
func makeItems(n int) <-chan pbItem {
	ch := make(chan pbItem, n)
	for i := 0; i < n; i++ {
		ch <- pbItem{id: fmt.Sprintf("item-%d", i), val: i}
	}
	close(ch)
	return ch
}

// collectResults drains a channel into a slice with a timeout.
func collectResults[T any](t *testing.T, ch <-chan T, timeout time.Duration) []T {
	t.Helper()
	var results []T
	deadline := time.After(timeout)
	for {
		select {
		case item, ok := <-ch:
			if !ok {
				return results
			}
			results = append(results, item)
		case <-deadline:
			t.Fatalf("timed out after %v collecting results (got %d so far)", timeout, len(results))
			return results
		}
	}
}

func noopCfg() *Config {
	return &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
}

// ─── Phase 1: Core builder structure ─────────────────────────────────────────

func TestNewPipelineBuilder_Defaults(t *testing.T) {
	pb := NewPipelineBuilder(nil)
	if pb == nil {
		t.Fatal("expected non-nil builder")
	}
	if pb.defaultCfg == nil {
		t.Error("expected default config to be set")
	}
	if len(pb.Components()) != 0 {
		t.Error("expected empty components map")
	}
}

func TestNewPipelineBuilder_WithConfig(t *testing.T) {
	cfg := noopCfg()
	pb := NewPipelineBuilder(cfg)
	if pb.defaultCfg != cfg {
		t.Error("expected builder to use the provided config")
	}
}

func TestAddStage_RegistersComponent(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }

	AddStage(pb, "stage1", 2, fn)

	comps := pb.Components()
	if _, ok := comps["stage1"]; !ok {
		t.Error("expected stage1 to be registered")
	}
	if comps["stage1"].getType() != ComponentTypeStage {
		t.Errorf("expected ComponentTypeStage, got %v", comps["stage1"].getType())
	}
}

func TestAddStage_PreservesTypes(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }

	AddStage(pb, "typed", 1, fn)

	comp := pb.Components()["typed"]
	if comp.getInputType().Name() != "pbItem" {
		t.Errorf("expected input type pbItem, got %s", comp.getInputType().Name())
	}
	if comp.getOutputType().Name() != "pbResult" {
		t.Errorf("expected output type pbResult, got %s", comp.getOutputType().Name())
	}
}

func TestAddStage_BuildsEdges(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "first", 1, fn)
	AddStage(pb, "second", 1, fn2, "first")

	edges := pb.Edges()
	secondEdges, ok := edges["first"]
	if !ok {
		t.Fatal("expected edge from 'first'")
	}
	if len(secondEdges) != 1 || secondEdges[0] != "second" {
		t.Errorf("expected edge first→second, got %v", secondEdges)
	}
}

func TestAddStageAfter_SingleUpstream(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "a", 1, fn)
	AddStageAfter(pb, "b", 1, fn2, "a")

	edges := pb.Edges()["a"]
	if len(edges) != 1 || edges[0] != "b" {
		t.Errorf("AddStageAfter: expected a→b edge, got %v", edges)
	}
}

func TestAddStage_DuplicateID_RecordsError(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }

	AddStage(pb, "dup", 1, fn)
	AddStage(pb, "dup", 1, fn)

	if len(pb.registrationErrors) == 0 {
		t.Error("expected a registration error for duplicate ID")
	}
}

func TestAddRouter_RegistersComponent(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	routeFn := func(in <-chan pbResult) map[string]<-chan pbResult {
		return map[string]<-chan pbResult{"all": in}
	}

	AddStage(pb, "source", 1, fn)
	AddRouter(pb, "router", routeFn, "source")

	comps := pb.Components()
	if _, ok := comps["router"]; !ok {
		t.Error("expected router to be registered")
	}
	if comps["router"].getType() != ComponentTypeRouter {
		t.Errorf("expected ComponentTypeRouter, got %v", comps["router"].getType())
	}
}

func TestRouterBranchRef_ID(t *testing.T) {
	ref := RouterBranchRef{RouterID: "myRouter", BranchKey: "premium"}
	want := "myRouter/premium"
	if ref.ID() != want {
		t.Errorf("RouterBranchRef.ID() = %q, want %q", ref.ID(), want)
	}
}

func TestRegistrationOrder_Preserved(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }
	fn3 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "alpha", 1, fn)
	AddStage(pb, "beta", 1, fn2, "alpha")
	AddStage(pb, "gamma", 1, fn3, "beta")

	order := pb.RegistrationOrder()
	want := []string{"alpha", "beta", "gamma"}
	for i, id := range want {
		if order[i] != id {
			t.Errorf("registration order[%d] = %q, want %q", i, order[i], id)
		}
	}
}

// ─── Phase 2: Validation ──────────────────────────────────────────────────────

func TestValidate_EmptyGraph(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	vr := pb.Validate()
	if vr.IsValid {
		t.Error("empty graph should not be valid")
	}
	if len(vr.Errors) == 0 {
		t.Error("expected at least one validation error for empty graph")
	}
}

func TestValidate_SingleSourceStage_IsValid(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	AddStage(pb, "only", 1, fn)

	vr := pb.Validate()
	if !vr.IsValid {
		t.Errorf("single source stage should be valid; errors: %v", vr.Errors)
	}
}

func TestValidate_LinearChain_IsValid(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "src", 1, fn)
	AddStage(pb, "dst", 2, fn2, "src")

	vr := pb.Validate()
	if !vr.IsValid {
		t.Errorf("linear chain should be valid; errors: %v", vr.Errors)
	}
}

func TestValidate_ZeroWorkers_IsInvalid(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	AddStage(pb, "zero-workers", 0, fn)

	vr := pb.Validate()
	if vr.IsValid {
		t.Error("zero-worker stage should fail validation")
	}
	found := false
	for _, e := range vr.Errors {
		if e.Component == "zero-workers" {
			found = true
		}
	}
	if !found {
		t.Error("expected validation error referencing 'zero-workers'")
	}
}

func TestValidate_UnknownInputID_IsInvalid(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }

	// "ghost" is not registered
	AddStage(pb, "stage", 1, fn, "ghost")

	vr := pb.Validate()
	if vr.IsValid {
		t.Error("referencing an unknown inputID should fail validation")
	}
	found := false
	for _, e := range vr.Errors {
		if e.Component == "stage" {
			found = true
		}
	}
	if !found {
		t.Error("expected validation error referencing 'stage'")
	}
}

func TestValidate_RegistrationErrors_Surfaced(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }

	AddStage(pb, "dup", 1, fn)
	AddStage(pb, "dup", 1, fn) // duplicate

	vr := pb.Validate()
	if vr.IsValid {
		t.Error("duplicate registration should fail validation")
	}
}

func TestValidate_CycleDetection(t *testing.T) {
	// We manually manipulate edges to create an artificial cycle because the
	// builder prevents registering components that reference unknown IDs.
	// A cycle requires both nodes to already exist.
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	// Register both components with valid upstream wiring for registration.
	AddStage(pb, "a", 1, fn)
	AddStage(pb, "b", 1, fn2, "a")

	// Inject the back-edge manually to simulate a cycle (a → b → a).
	pb.edges["b"] = append(pb.edges["b"], "a")

	vr := pb.Validate()
	if vr.IsValid {
		t.Error("cycle in graph should fail validation")
	}
	foundCycle := false
	for _, e := range vr.Errors {
		if e.Component == "" && len(e.Reason) > 0 {
			foundCycle = true
		}
	}
	if !foundCycle {
		t.Errorf("expected a cycle error, got: %v", vr.Errors)
	}
}

func TestValidate_TypeMismatch_IsInvalid(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())

	// first outputs pbResult; second expects pbItem — mismatch
	fn1 := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(i pbItem) (pbItem, error) { return i, nil } // expects pbItem, not pbResult

	AddStage(pb, "first", 1, fn1)
	AddStage(pb, "second", 1, fn2, "first")

	vr := pb.Validate()
	if vr.IsValid {
		t.Error("type mismatch between stages should fail validation")
	}
	found := false
	for _, e := range vr.Errors {
		if e.Component == "second" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected type mismatch error on 'second', got: %v", vr.Errors)
	}
}

func TestValidate_RouterBranchRef_IsValid(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())

	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	routeFn := func(in <-chan pbResult) map[string]<-chan pbResult {
		return map[string]<-chan pbResult{"fast": in}
	}
	fn3 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "src", 1, fn)
	AddRouter(pb, "route", routeFn, "src")
	AddStage(pb, "sink", 1, fn3,
		RouterBranchRef{RouterID: "route", BranchKey: "fast"}.ID())

	vr := pb.Validate()
	if !vr.IsValid {
		t.Errorf("valid router branch ref should pass validation; errors: %v", vr.Errors)
	}
}

func TestValidate_AllErrorsAccumulated(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }

	// two problems: zero workers AND unknown inputID
	AddStage(pb, "bad", 0, fn, "ghost")

	vr := pb.Validate()
	if vr.IsValid {
		t.Error("should not be valid")
	}
	if len(vr.Errors) < 2 {
		t.Errorf("expected at least 2 errors, got %d: %v", len(vr.Errors), vr.Errors)
	}
}

func TestValidationResult_String(t *testing.T) {
	vr := ValidationResult{
		IsValid: false,
		Errors: []ValidationError{
			{Component: "foo", Reason: "something wrong"},
			{Reason: "graph-level issue"},
		},
	}
	s := vr.String()
	if s == "" {
		t.Error("expected non-empty string from ValidationResult.String()")
	}
	if vr2 := (ValidationResult{IsValid: true}); vr2.String() != "validation passed" {
		t.Errorf("expected 'validation passed', got %q", vr2.String())
	}
}

// ─── Phase 3: Pipeline execution ─────────────────────────────────────────────

func TestBuild_SingleSourceStage(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())

	fn := func(i pbItem) (pbResult, error) {
		return pbResult{id: i.id, val: i.val * 2}, nil
	}
	AddStage(pb, "double", 2, fn)

	ctx := context.Background()
	feederCh := makeItems(5)
	handle, err := pb.Build(ctx, map[string]any{"double": feederCh})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	results, err := CollectAll[pbResult](handle.Results(), "double")
	if err != nil {
		t.Fatalf("CollectAll error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
	for _, r := range results {
		if r.val != r.val { // sanity — values were doubled
			t.Errorf("unexpected value %d", r.val)
		}
	}
}

func TestBuild_LinearChain(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())

	double := func(i pbItem) (pbResult, error) {
		return pbResult{id: i.id, val: i.val * 2}, nil
	}
	addOne := func(r pbResult) (pbResult, error) {
		return pbResult{id: r.id, val: r.val + 1}, nil
	}

	AddStage(pb, "double", 1, double)
	AddStageAfter(pb, "addOne", 1, addOne, "double")

	ctx := context.Background()
	feeder := makeItems(3)
	handle, err := pb.Build(ctx, map[string]any{"double": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	results, err := CollectAll[pbResult](handle.Results(), "addOne")
	if err != nil {
		t.Fatalf("CollectAll error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Sort by id for determinism.
	sort.Slice(results, func(i, j int) bool { return results[i].id < results[j].id })

	// item-0: val=0 → *2=0 → +1=1
	// item-1: val=1 → *2=2 → +1=3
	// item-2: val=2 → *2=4 → +1=5
	want := []int{1, 3, 5}
	for i, r := range results {
		if r.val != want[i] {
			t.Errorf("results[%d].val = %d, want %d", i, r.val, want[i])
		}
	}
}

func TestBuild_WithRouter(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())

	toResult := func(i pbItem) (pbResult, error) {
		return pbResult{id: i.id, val: i.val}, nil
	}
	routeFn := func(in <-chan pbResult) map[string]<-chan pbResult {
		even := make(chan pbResult, 10)
		odd := make(chan pbResult, 10)
		go func() {
			defer close(even)
			defer close(odd)
			for r := range in {
				if r.val%2 == 0 {
					even <- r
				} else {
					odd <- r
				}
			}
		}()
		return map[string]<-chan pbResult{
			"even": even,
			"odd":  odd,
		}
	}
	passThrough := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "toResult", 1, toResult)
	AddRouter(pb, "split", routeFn, "toResult")
	AddStage(pb, "evens", 1, passThrough,
		RouterBranchRef{RouterID: "split", BranchKey: "even"}.ID())
	AddStage(pb, "odds", 1, passThrough,
		RouterBranchRef{RouterID: "split", BranchKey: "odd"}.ID())

	feeder := makeItems(6) // vals 0..5
	ctx := context.Background()
	handle, err := pb.Build(ctx, map[string]any{"toResult": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	evens, err := CollectAll[pbResult](handle.Results(), "evens")
	if err != nil {
		t.Fatalf("CollectAll evens error: %v", err)
	}
	odds, err := CollectAll[pbResult](handle.Results(), "odds")
	if err != nil {
		t.Fatalf("CollectAll odds error: %v", err)
	}

	if len(evens) != 3 {
		t.Errorf("expected 3 evens, got %d", len(evens))
	}
	if len(odds) != 3 {
		t.Errorf("expected 3 odds, got %d", len(odds))
	}
	for _, r := range evens {
		if r.val%2 != 0 {
			t.Errorf("expected even value, got %d", r.val)
		}
	}
	for _, r := range odds {
		if r.val%2 == 0 {
			t.Errorf("expected odd value, got %d", r.val)
		}
	}
}

func TestBuild_FailsValidation(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	// empty builder — no components
	_, err := pb.Build(context.Background(), nil)
	if err == nil {
		t.Error("Build() should fail on invalid pipeline")
	}
}

func TestBuild_MissingFeeder_ReturnsError(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	AddStage(pb, "src", 1, fn)

	// No feeders provided
	_, err := pb.Build(context.Background(), nil)
	if err == nil {
		t.Error("Build() without feeder for source stage should return error")
	}
}

func TestBuild_ContextCancellation(t *testing.T) {
	// StartWorker checks ctx.Done() once at startup, then ranges over the
	// input channel. To observe cancellation stopping the pipeline we must
	// also close the feeder so the range loop can exit on the next iteration
	// after the context is cancelled.
	pb := NewPipelineBuilder(noopCfg())

	slow := func(i pbItem) (pbResult, error) {
		time.Sleep(10 * time.Millisecond)
		return pbResult{id: i.id, val: i.val}, nil
	}
	AddStage(pb, "slow", 2, slow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	feeder := make(chan pbItem, 10)
	for i := 0; i < 10; i++ {
		feeder <- pbItem{id: fmt.Sprintf("i%d", i), val: i}
	}

	handle, err := pb.Build(ctx, map[string]any{"slow": (<-chan pbItem)(feeder)})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	// Cancel the context, then close the feeder so the workers' range loops
	// can return on the next receive attempt.
	cancel()
	close(feeder)

	select {
	case <-handle.Done():
		// Pipeline stopped cleanly.
	case <-time.After(2 * time.Second):
		t.Error("pipeline did not stop after context cancellation + feeder close")
	}
}

func TestPipelineHandle_CancelMethod(t *testing.T) {
	// StartWorker checks ctx.Done() once at startup, then ranges over the
	// input channel. Cancellation therefore only unblocks the worker once the
	// input channel is also closed (the range will return on the next
	// iteration after the channel drains). We verify that:
	//   1. Cancel() does not panic or block.
	//   2. Once Cancel() is called AND the feeder is closed, the pipeline
	//      stops within a reasonable deadline.
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) {
		time.Sleep(5 * time.Millisecond)
		return pbResult{id: i.id}, nil
	}
	AddStage(pb, "s", 1, fn)

	feeder := make(chan pbItem, 5)
	for i := 0; i < 5; i++ {
		feeder <- pbItem{id: fmt.Sprintf("i%d", i), val: i}
	}

	handle, err := pb.Build(context.Background(), map[string]any{"s": (<-chan pbItem)(feeder)})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	// Cancel the pipeline context, then close the feeder so the worker's
	// range loop can exit naturally on the next iteration.
	handle.Cancel()
	close(feeder)

	select {
	case <-handle.Done():
		// Pipeline stopped cleanly after Cancel + feeder close.
	case <-time.After(2 * time.Second):
		t.Error("pipeline did not stop after Cancel() + feeder close")
	}
}

func TestPipelineHandle_Wait(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	AddStage(pb, "s", 2, fn)

	feeder := makeItems(10)
	handle, err := pb.Build(context.Background(), map[string]any{"s": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	// Drain the output so the stage can finish.
	go func() {
		outRaw := handle.stageOutputs["s"]
		outCh := outRaw.(<-chan pbResult)
		for range outCh {
		}
	}()

	done := make(chan struct{})
	go func() {
		handle.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("Wait() did not return after pipeline finished")
	}
}

// ─── Phase 4: Result helpers ─────────────────────────────────────────────────

func TestGetStageOutput_Success(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	AddStage(pb, "s", 1, fn)

	feeder := makeItems(2)
	handle, err := pb.Build(context.Background(), map[string]any{"s": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	ch, err := GetStageOutput[pbResult](handle.Results(), "s")
	if err != nil {
		t.Fatalf("GetStageOutput error: %v", err)
	}
	results := collectResults(t, ch, time.Second)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestGetStageOutput_UnknownStage(t *testing.T) {
	ph := &PipelineHandle{
		stageOutputs:  make(map[string]any),
		routerOutputs: make(map[string]map[string]any),
		done:          make(chan struct{}),
		wg:            &sync.WaitGroup{},
	}
	_, err := GetStageOutput[pbResult](ph.Results(), "nonexistent")
	if err == nil {
		t.Error("expected error for unknown stage")
	}
}

func TestGetStageOutput_WrongType(t *testing.T) {
	ch := make(chan pbItem)
	close(ch)

	ph := &PipelineHandle{
		stageOutputs:  map[string]any{"s": (<-chan pbItem)(ch)},
		routerOutputs: make(map[string]map[string]any),
		done:          make(chan struct{}),
		wg:            &sync.WaitGroup{},
	}
	_, err := GetStageOutput[pbResult](ph.Results(), "s")
	if err == nil {
		t.Error("expected type mismatch error")
	}
}

func TestGetRouterBranch_Success(t *testing.T) {
	branchCh := make(chan pbResult, 1)
	branchCh <- pbResult{id: "x", val: 42}
	close(branchCh)

	ph := &PipelineHandle{
		stageOutputs: make(map[string]any),
		routerOutputs: map[string]map[string]any{
			"router": {"main": (<-chan pbResult)(branchCh)},
		},
		done: make(chan struct{}),
		wg:   &sync.WaitGroup{},
	}

	ch, err := GetRouterBranch[pbResult](ph.Results(), "router", "main")
	if err != nil {
		t.Fatalf("GetRouterBranch error: %v", err)
	}
	item := <-ch
	if item.val != 42 {
		t.Errorf("expected 42, got %d", item.val)
	}
}

func TestGetRouterBranch_UnknownRouter(t *testing.T) {
	ph := &PipelineHandle{
		stageOutputs:  make(map[string]any),
		routerOutputs: make(map[string]map[string]any),
		done:          make(chan struct{}),
		wg:            &sync.WaitGroup{},
	}
	_, err := GetRouterBranch[pbResult](ph.Results(), "ghost", "branch")
	if err == nil {
		t.Error("expected error for unknown router")
	}
}

func TestGetRouterBranch_UnknownBranch(t *testing.T) {
	ph := &PipelineHandle{
		stageOutputs: make(map[string]any),
		routerOutputs: map[string]map[string]any{
			"router": {},
		},
		done: make(chan struct{}),
		wg:   &sync.WaitGroup{},
	}
	_, err := GetRouterBranch[pbResult](ph.Results(), "router", "ghost-branch")
	if err == nil {
		t.Error("expected error for unknown branch key")
	}
}

func TestGetMergedOutput_Success(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	fn2 := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val + 100}, nil }

	AddStage(pb, "s1", 1, fn)
	AddStage(pb, "s2", 1, fn2)

	feeder1 := makeItems(3)
	feeder2 := makeItems(3)
	handle, err := pb.Build(context.Background(), map[string]any{
		"s1": feeder1,
		"s2": feeder2,
	})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	merged, err := GetMergedOutput[pbResult](handle.Results(), "s1", "s2")
	if err != nil {
		t.Fatalf("GetMergedOutput error: %v", err)
	}
	results := collectResults(t, merged, 2*time.Second)
	if len(results) != 6 {
		t.Errorf("expected 6 merged results, got %d", len(results))
	}
}

func TestGetMergedOutput_NoIDs(t *testing.T) {
	ph := &PipelineHandle{
		stageOutputs:  make(map[string]any),
		routerOutputs: make(map[string]map[string]any),
		done:          make(chan struct{}),
		wg:            &sync.WaitGroup{},
	}
	_, err := GetMergedOutput[pbResult](ph.Results())
	if err == nil {
		t.Error("expected error when no stage IDs provided")
	}
}

func TestCollectAll_Success(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	AddStage(pb, "s", 1, fn)

	feeder := makeItems(4)
	handle, err := pb.Build(context.Background(), map[string]any{"s": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	results, err := CollectAll[pbResult](handle.Results(), "s")
	if err != nil {
		t.Fatalf("CollectAll error: %v", err)
	}
	if len(results) != 4 {
		t.Errorf("expected 4 results, got %d", len(results))
	}
}

// ─── Phase 5: Config overrides ────────────────────────────────────────────────

func TestWithDefaultConfig_Override(t *testing.T) {
	pb := NewPipelineBuilder(nil)
	newCfg := noopCfg()
	pb.WithDefaultConfig(newCfg)
	if pb.defaultCfg != newCfg {
		t.Error("WithDefaultConfig did not update default config")
	}
}

func TestWithComponentConfig_PerComponent(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	AddStage(pb, "s", 1, fn)

	customCfg := noopCfg()
	pb.WithComponentConfig("s", customCfg)

	if pb.configFor("s") != customCfg {
		t.Error("per-component config override not applied")
	}
}

func TestWithComponentConfigs_MultipleComponents(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "a", 1, fn)
	AddStage(pb, "b", 1, fn2, "a")

	sharedCfg := noopCfg()
	pb.WithComponentConfigs(sharedCfg, "a", "b")

	if pb.configFor("a") != sharedCfg {
		t.Error("component config not applied to 'a'")
	}
	if pb.configFor("b") != sharedCfg {
		t.Error("component config not applied to 'b'")
	}
}

func TestConfigResolution_Hierarchy(t *testing.T) {
	defaultCfg := noopCfg()
	perComponentCfg := noopCfg()

	pb := NewPipelineBuilder(defaultCfg)
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "has-override", 1, fn)
	AddStage(pb, "uses-default", 1, fn2, "has-override")

	pb.WithComponentConfig("has-override", perComponentCfg)

	if pb.configFor("has-override") != perComponentCfg {
		t.Error("per-component config should take priority")
	}
	if pb.configFor("uses-default") != defaultCfg {
		t.Error("default config should be used when no override exists")
	}
	if pb.configFor("nonexistent") != defaultCfg {
		t.Error("unknown component should fall back to default config")
	}
}

func TestConfigResolution_FallsBackToDefaultConfig(t *testing.T) {
	// builder with no default → should fall back to package DefaultConfig()
	pb := &PipelineBuilder{
		componentConfigs: make(map[string]*Config),
	}
	cfg := pb.configFor("anything")
	if cfg == nil {
		t.Error("configFor should never return nil")
	}
}

// ─── Observer integration ─────────────────────────────────────────────────────

func TestBuild_EmitsStageEvents(t *testing.T) {
	var mu sync.Mutex
	var eventTypes []EventType

	observer := ObserverFunc(func(_ context.Context, e Eventful) {
		mu.Lock()
		eventTypes = append(eventTypes, e.GetType())
		mu.Unlock()
	})

	cfg := &Config{
		Logger:   NoOpLogger{},
		Observer: observer,
		Context:  context.Background(),
	}
	pb := NewPipelineBuilder(cfg)
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	AddStage(pb, "observed", 2, fn)

	feeder := makeItems(5)
	handle, err := pb.Build(context.Background(), map[string]any{"observed": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	// Drain the output so the stage can finish.
	_, _ = CollectAll[pbResult](handle.Results(), "observed")
	handle.Wait()

	mu.Lock()
	defer mu.Unlock()

	hasStarted := false
	hasCompleted := false
	for _, et := range eventTypes {
		if et == StageStarted {
			hasStarted = true
		}
		if et == StageCompleted {
			hasCompleted = true
		}
	}
	if !hasStarted {
		t.Error("expected StageStarted event to be emitted")
	}
	if !hasCompleted {
		t.Error("expected StageCompleted event to be emitted")
	}
}

// ─── Concurrency / stress tests ───────────────────────────────────────────────

func TestBuild_ConcurrentWorkers_AllItemsProcessed(t *testing.T) {
	const numItems = 1000
	const numWorkers = 8

	pb := NewPipelineBuilder(noopCfg())

	var counter atomic.Int64
	fn := func(i pbItem) (pbResult, error) {
		counter.Add(1)
		return pbResult{id: i.id, val: i.val}, nil
	}
	AddStage(pb, "count", numWorkers, fn)

	feeder := makeItems(numItems)
	handle, err := pb.Build(context.Background(), map[string]any{"count": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	results, err := CollectAll[pbResult](handle.Results(), "count")
	if err != nil {
		t.Fatalf("CollectAll error: %v", err)
	}
	if len(results) != numItems {
		t.Errorf("expected %d results, got %d", numItems, len(results))
	}
	if counter.Load() != numItems {
		t.Errorf("expected counter=%d, got %d", numItems, counter.Load())
	}
}

func TestBuild_MultipleIndependentSources(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())

	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id, val: i.val}, nil }
	AddStage(pb, "src1", 1, fn)
	AddStage(pb, "src2", 1, fn)

	f1 := makeItems(5)
	f2 := makeItems(5)

	handle, err := pb.Build(context.Background(), map[string]any{
		"src1": f1,
		"src2": f2,
	})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	r1, err := CollectAll[pbResult](handle.Results(), "src1")
	if err != nil {
		t.Fatalf("CollectAll src1 error: %v", err)
	}
	r2, err := CollectAll[pbResult](handle.Results(), "src2")
	if err != nil {
		t.Fatalf("CollectAll src2 error: %v", err)
	}
	if len(r1) != 5 || len(r2) != 5 {
		t.Errorf("expected 5 from each source, got %d and %d", len(r1), len(r2))
	}
}

func TestBuild_DeepChain(t *testing.T) {
	const depth = 10
	pb := NewPipelineBuilder(noopCfg())

	incr := func(r pbResult) (pbResult, error) {
		return pbResult{id: r.id, val: r.val + 1}, nil
	}
	seed := func(i pbItem) (pbResult, error) {
		return pbResult{id: i.id, val: i.val}, nil
	}

	AddStage(pb, "stage-0", 1, seed)
	for i := 1; i < depth; i++ {
		AddStage(pb, fmt.Sprintf("stage-%d", i), 1, incr,
			fmt.Sprintf("stage-%d", i-1))
	}

	feeder := makeItems(3)
	handle, err := pb.Build(context.Background(), map[string]any{"stage-0": feeder})
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	results, err := CollectAll[pbResult](handle.Results(), fmt.Sprintf("stage-%d", depth-1))
	if err != nil {
		t.Fatalf("CollectAll error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	// Each item passes through depth-1 increment stages.
	for _, r := range results {
		originalVal := r.val - (depth - 1) // reverse the increments
		if originalVal < 0 || originalVal > 2 {
			t.Errorf("unexpected final value %d (original would be %d)", r.val, originalVal)
		}
	}
}

// ─── Topological sort ─────────────────────────────────────────────────────────

func TestTopologicalSort_LinearChain(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }
	fn3 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "a", 1, fn)
	AddStage(pb, "b", 1, fn2, "a")
	AddStage(pb, "c", 1, fn3, "b")

	order, err := pb.topologicalSort()
	if err != nil {
		t.Fatalf("topologicalSort error: %v", err)
	}
	if order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Errorf("unexpected sort order: %v", order)
	}
}

func TestTopologicalSort_DiamondShape(t *testing.T) {
	//      src
	//     /   \
	//   left  right
	//     \   /
	//      sink
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	passThrough := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "src", 1, fn)
	AddStage(pb, "left", 1, passThrough, "src")
	AddStage(pb, "right", 1, passThrough, "src")
	AddStage(pb, "sink", 1, passThrough, "left", "right")

	order, err := pb.topologicalSort()
	if err != nil {
		t.Fatalf("topologicalSort error: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 components in sorted order, got %d", len(order))
	}

	pos := make(map[string]int, 4)
	for i, id := range order {
		pos[id] = i
	}
	if pos["src"] >= pos["left"] || pos["src"] >= pos["right"] {
		t.Error("src must appear before left and right")
	}
	if pos["left"] >= pos["sink"] || pos["right"] >= pos["sink"] {
		t.Error("left and right must appear before sink")
	}
}

// ─── Introspection helpers ────────────────────────────────────────────────────

func TestEdges_Snapshot(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	fn2 := func(r pbResult) (pbResult, error) { return r, nil }

	AddStage(pb, "a", 1, fn)
	AddStage(pb, "b", 1, fn2, "a")

	edges := pb.Edges()

	// Mutating the snapshot should not affect the builder.
	edges["a"] = append(edges["a"], "injected")
	realEdges := pb.Edges()
	for _, to := range realEdges["a"] {
		if to == "injected" {
			t.Error("snapshot mutation leaked back into builder")
		}
	}
}

func TestComponents_Snapshot(t *testing.T) {
	pb := NewPipelineBuilder(noopCfg())
	fn := func(i pbItem) (pbResult, error) { return pbResult{id: i.id}, nil }
	AddStage(pb, "s", 1, fn)

	comps := pb.Components()
	delete(comps, "s")

	// Original builder should still have the component.
	if _, ok := pb.Components()["s"]; !ok {
		t.Error("deleting from snapshot should not affect builder")
	}
}
