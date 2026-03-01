package gopl

import (
	"errors"
	"testing"
	"time"
)

type testItem struct {
	id  string
	val int
}

func (t testItem) GetID() string {
	return t.id
}

// testOutput wraps an int so it satisfies the Identifier interface.
type testOutput struct {
	id  string
	val int
}

func (o testOutput) GetID() string { return o.id }

func TestStartStage_Success(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
	input := make(chan testItem, 1)
	input <- testItem{id: "1", val: 1}
	close(input)

	fn := func(item testItem) (testOutput, error) {
		return testOutput{id: item.id, val: item.val * 2}, nil
	}

	out, err := StartStage(config, "test-stage", 1, fn, input)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-out:
		if result.val != 2 {
			t.Errorf("Expected val=2, got %d", result.val)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for result")
	}
}

func TestStartStage_Error(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
	input := make(chan testItem, 1)
	input <- testItem{id: "1", val: 1}
	close(input)

	fn := func(item testItem) (testOutput, error) {
		return testOutput{}, errors.New("test error")
	}

	out, err := StartStage(config, "test-stage", 1, fn, input)
	if err != nil {
		t.Fatal(err)
	}

	// Channel should still close, but no output due to error
	select {
	case result, ok := <-out:
		if ok {
			t.Errorf("Expected no output on error, got %+v", result)
		}
		// closed channel – that's fine
	case <-time.After(100 * time.Millisecond):
		// Also acceptable: the stage closed without emitting anything
	}
}

func TestStartStage_NoInputChannels(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
	fn := func(item testItem) (testOutput, error) { return testOutput{}, nil }

	_, err := StartStage(config, "test-stage", 1, fn)
	if err == nil {
		t.Error("Expected error for no input channels")
	}
}

func TestStartStage_MultipleInputs(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
	input1 := make(chan testItem, 1)
	input2 := make(chan testItem, 1)
	input1 <- testItem{id: "1", val: 1}
	input2 <- testItem{id: "2", val: 2}
	close(input1)
	close(input2)

	fn := func(item testItem) (testOutput, error) {
		return testOutput{id: item.id, val: item.val}, nil
	}

	out, err := StartStage(config, "test-stage", 1, fn, input1, input2)
	if err != nil {
		t.Fatal(err)
	}

	results := make(map[int]bool)
	for i := 0; i < 2; i++ {
		select {
		case result := <-out:
			results[result.val] = true
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for results")
		}
	}

	if !results[1] || !results[2] {
		t.Errorf("Expected both values 1 and 2, got %v", results)
	}
}

func TestStartStageConfig_Success(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
	input := make(chan testItem, 2)
	input <- testItem{id: "a", val: 10}
	input <- testItem{id: "b", val: 20}
	close(input)

	fn := func(item testItem) (testOutput, error) {
		return testOutput{id: item.id, val: item.val + 1}, nil
	}

	sc := NewStageConfig(config, "cfg-stage", 2, fn, input)
	out, err := StartStageConfig(sc)
	if err != nil {
		t.Fatal(err)
	}

	collected := make(map[string]int)
	for i := 0; i < 2; i++ {
		select {
		case result := <-out:
			collected[result.id] = result.val
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for result")
		}
	}

	if collected["a"] != 11 {
		t.Errorf("Expected a=11, got %d", collected["a"])
	}
	if collected["b"] != 21 {
		t.Errorf("Expected b=21, got %d", collected["b"])
	}
}

func TestStageBuilder_Build(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}
	input := make(chan testItem, 1)
	input <- testItem{id: "z", val: 99}
	close(input)

	fn := func(item testItem) (testOutput, error) {
		return testOutput{id: item.id, val: item.val}, nil
	}

	sc, err := NewStageBuilder[testItem, testOutput]().
		WithConfig(config).
		WithStageId("builder-stage").
		WithWorkers(1).
		WithFunction(fn).
		WithChannels(input).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	out, err := sc.Start()
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	select {
	case result := <-out:
		if result.val != 99 {
			t.Errorf("Expected val=99, got %d", result.val)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for result")
	}
}

func TestStageBuilder_MissingFields(t *testing.T) {
	fn := func(item testItem) (testOutput, error) { return testOutput{}, nil }

	// missing config
	_, err := NewStageBuilder[testItem, testOutput]().
		WithStageId("x").WithWorkers(1).WithFunction(fn).
		Build()
	if err == nil {
		t.Error("Expected error for missing config")
	}

	config := &Config{Logger: NoOpLogger{}, Observer: NoOpObserver{}}

	// missing stageId
	_, err = NewStageBuilder[testItem, testOutput]().
		WithConfig(config).WithWorkers(1).WithFunction(fn).
		Build()
	if err == nil {
		t.Error("Expected error for missing stageId")
	}

	// missing workers
	_, err = NewStageBuilder[testItem, testOutput]().
		WithConfig(config).WithStageId("x").WithFunction(fn).
		Build()
	if err == nil {
		t.Error("Expected error for missing workers")
	}

	// missing function
	_, err = NewStageBuilder[testItem, testOutput]().
		WithConfig(config).WithStageId("x").WithWorkers(1).
		Build()
	if err == nil {
		t.Error("Expected error for missing function")
	}

	// missing channels
	_, err = NewStageBuilder[testItem, testOutput]().
		WithConfig(config).WithStageId("x").WithWorkers(1).WithFunction(fn).
		Build()
	if err == nil {
		t.Error("Expected error for missing channels")
	}
}
