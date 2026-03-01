package gopl

import (
	"sync"
)

// StringItem wraps a plain string so it satisfies the Identifier interface.
// Use it whenever a stage needs to output or receive string values.
type StringItem struct {
	ID    string
	Value string
}

func (s StringItem) GetID() string { return s.ID }

// NewStringItem creates a StringItem whose ID and Value are both set to v.
func NewStringItem(v string) StringItem {
	return StringItem{ID: v, Value: v}
}

// NewStringItemWithID creates a StringItem with an explicit ID and value.
func NewStringItemWithID(id, value string) StringItem {
	return StringItem{ID: id, Value: value}
}

// Merge merges multiple channels into a single channel.
func Merge[T any](cs ...<-chan T) <-chan T {
	out := make(chan T)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan T) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
