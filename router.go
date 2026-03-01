package gopl

import (
	"fmt"
	"reflect"
)


// ForkUnbuffered creates two unbuffered channels from a single input channel based on type assertion
func ForkUnbuffered[T, U any](c <-chan any) (left chan T, right chan U) {
	left = make(chan T)
	right = make(chan U)
	go func() {
		defer close(left)
		defer close(right)
		for v := range c {
			switch v := v.(type) {
			case T:
				left <- v
			case U:
				right <- v
			default:
				panic(fmt.Sprintf("unsupported type: %T", v)) // unexpected type
			}
		}
	}()
	return left, right
}

// Fork creates two buffered channels from a single input channel based on type assertion
func Fork[T, U any](c <-chan any, leftBufSize, rightBufSize int) (left chan T, right chan U) {
	left = make(chan T, leftBufSize)
	right = make(chan U, rightBufSize)
	go func() {
		defer close(left)
		defer close(right)
		for v := range c {
			switch v := v.(type) {
			case T:
				left <- v
			case U:
				right <- v
			default:
				panic(fmt.Sprintf("unsupported type: %T", v)) // unexpected type
			}
		}
	}()
	return left, right
}

// RouteByPredicate routes items to different channels based on predicate functions
// Each predicate function determines if an item should go to the corresponding channel
func RouteByPredicate[T any](input <-chan T, bufferSizes []int, predicates ...func(T) bool) []chan T {
	if len(predicates) != len(bufferSizes) {
		panic("number of predicates must match number of buffer sizes")
	}

	outputs := make([]chan T, len(predicates))
	for i, bufSize := range bufferSizes {
		outputs[i] = make(chan T, bufSize)
	}

	go func() {
		defer func() {
			for _, ch := range outputs {
				close(ch)
			}
		}()

		for item := range input {
			routed := false
			for i, predicate := range predicates {
				if predicate(item) {
					outputs[i] <- item
					routed = true
					break // Route to first matching predicate only
				}
			}
			if !routed {
				panic(fmt.Sprintf("no predicate matched for item: %+v", item))
			}
		}
	}()

	return outputs
}

// RouteByKey routes items to channels based on a key extraction function
// The key function extracts a routing key from each item
func RouteByKey[T any, K comparable](input <-chan T, bufferSize int, keyFunc func(T) K, keys ...K) map[K]chan T {
	outputs := make(map[K]chan T)
	for _, key := range keys {
		outputs[key] = make(chan T, bufferSize)
	}

	go func() {
		defer func() {
			for _, ch := range outputs {
				close(ch)
			}
		}()

		for item := range input {
			key := keyFunc(item)
			if ch, exists := outputs[key]; exists {
				ch <- item
			} else {
				panic(fmt.Sprintf("no output channel for key: %v", key))
			}
		}
	}()

	return outputs
}

// MultiTypeRoute routes items to different channels based on their types
// Uses reflection to determine the appropriate channel for each type
func MultiTypeRoute(input <-chan any, bufferSize int, types ...reflect.Type) map[reflect.Type]chan any {
	outputs := make(map[reflect.Type]chan any)
	for _, t := range types {
		outputs[t] = make(chan any, bufferSize)
	}

	go func() {
		defer func() {
			for _, ch := range outputs {
				close(ch)
			}
		}()

		for item := range input {
			itemType := reflect.TypeOf(item)
			if ch, exists := outputs[itemType]; exists {
				ch <- item
			} else {
				panic(fmt.Sprintf("unsupported type: %T", item))
			}
		}
	}()

	return outputs
}

// ConditionalRoute routes items based on a condition function
// Items matching the condition go to the 'match' channel, others to 'nomatch'
func ConditionalRoute[T any](input <-chan T, condition func(T) bool, matchBufSize, nomatchBufSize int) (match, nomatch chan T) {
	match = make(chan T, matchBufSize)
	nomatch = make(chan T, nomatchBufSize)

	go func() {
		defer close(match)
		defer close(nomatch)

		for item := range input {
			if condition(item) {
				match <- item
			} else {
				nomatch <- item
			}
		}
	}()

	return match, nomatch
}
