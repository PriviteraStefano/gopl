package pipeline

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

func TestStartStage_Success(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	input := make(chan testItem, 1)
	input <- testItem{id: "1", val: 1}
	close(input)

	fn := func(item testItem) (int, error) {
		return item.val * 2, nil
	}

	out, err := StartStage(config, "test-stage", 1, fn, input)
	if err != nil {
		t.Fatal(err)
	}

	result := <-out
	if result != 2 {
		t.Errorf("Expected 2, got %d", result)
	}
}

func TestStartStage_Error(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	input := make(chan testItem, 1)
	input <- testItem{id: "1", val: 1}
	close(input)

	fn := func(item testItem) (int, error) {
		return 0, errors.New("test error")
	}

	out, err := StartStage(config, "test-stage", 1, fn, input)
	if err != nil {
		t.Fatal(err)
	}

	// Channel should still close, but no output due to error
	select {
	case <-out:
		t.Error("Expected no output on error")
	case <-time.After(100 * time.Millisecond):
		// Expected timeout
	}
}

func TestStartStage_NoInputChannels(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	fn := func(item testItem) (int, error) { return 0, nil }

	_, err := StartStage(config, "test-stage", 1, fn)
	if err == nil {
		t.Error("Expected error for no input channels")
	}
}

func TestStartStage_MultipleInputs(t *testing.T) {
	config := &Config{Logger: NoOpLogger{}}
	input1 := make(chan testItem, 1)
	input2 := make(chan testItem, 1)
	input1 <- testItem{id: "1", val: 1}
	input2 <- testItem{id: "2", val: 2}
	close(input1)
	close(input2)

	fn := func(item testItem) (int, error) {
		return item.val, nil
	}

	out, err := StartStage(config, "test-stage", 1, fn, input1, input2)
	if err != nil {
		t.Fatal(err)
	}

	results := make(map[int]bool)
	for i := 0; i < 2; i++ {
		select {
		case result := <-out:
			results[result] = true
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for results")
		}
	}

	if !results[1] || !results[2] {
		t.Error("Expected both values 1 and 2")
	}
}
