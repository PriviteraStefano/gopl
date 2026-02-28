package pipeline

import (
	"reflect"
	"testing"
	"time"
)

func TestForkUnbuffered(t *testing.T) {
	input := make(chan any, 2)
	input <- 1
	input <- "hello"
	close(input)

	left, right := ForkUnbuffered[int, string](input)

	select {
	case val := <-left:
		if val != 1 {
			t.Errorf("Expected 1, got %v", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout on left channel")
	}

	select {
	case val := <-right:
		if val != "hello" {
			t.Errorf("Expected 'hello', got %v", val)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout on right channel")
	}
}

func TestFork_Buffered(t *testing.T) {
	input := make(chan any, 2)
	input <- 1
	input <- "hello"
	close(input)

	left, right := Fork[int, string](input, 1, 1)

	if <-left != 1 {
		t.Error("Expected 1 on left")
	}
	if <-right != "hello" {
		t.Error("Expected 'hello' on right")
	}
}

func TestRouteByPredicate(t *testing.T) {
	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	outputs := RouteByPredicate(input, []int{1, 1}, func(x int) bool { return x%2 == 0 }, func(x int) bool { return x%2 != 0 })

	even := <-outputs[0]
	odd1 := <-outputs[1]
	odd2 := <-outputs[1]

	if even != 2 {
		t.Errorf("Expected 2, got %d", even)
	}
	if odd1 != 1 || odd2 != 3 {
		t.Error("Expected odds 1 and 3")
	}
}

func TestRouteByKey(t *testing.T) {
	input := make(chan int, 2)
	input <- 1
	input <- 2
	close(input)

	outputs := RouteByKey(input, 1, func(x int) string { return "even" }, "even", "odd")

	if <-outputs["even"] != 1 {
		t.Error("Expected 1 on even")
	}
	if <-outputs["odd"] != 2 {
		t.Error("Expected 2 on odd")
	}
}

func TestMultiTypeRoute(t *testing.T) {
	input := make(chan any, 2)
	input <- 1
	input <- "hello"
	close(input)

	outputs := MultiTypeRoute(input, 1, reflect.TypeFor[int](), reflect.TypeFor[string]())

	if <-outputs[reflect.TypeFor[int]()] != 1 {
		t.Error("Expected 1 on int channel")
	}
	if <-outputs[reflect.TypeFor[string]()] != "hello" {
		t.Error("Expected 'hello' on string channel")
	}
}

func TestConditionalRoute(t *testing.T) {
	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	match, nomatch := ConditionalRoute(input, func(x int) bool { return x > 2 }, 1, 1)

	if <-match != 3 {
		t.Error("Expected 3 on match")
	}
	if <-nomatch != 1 || <-nomatch != 2 {
		t.Error("Expected 1 and 2 on nomatch")
	}
}
