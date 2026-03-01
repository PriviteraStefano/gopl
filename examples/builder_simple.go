//go:build ignore

// builder_simple.go demonstrates the most basic PipelineBuilder usage:
// a linear chain of two stages where the first doubles each item's value
// and the second increments it by one.
//
// Run with:
//
//	go run pipeline/examples/builder_simple.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"pipeline"
)

// ─── Domain types ─────────────────────────────────────────────────────────────

type Number struct {
	ID    string
	Value int
}

func (n Number) GetID() string { return n.ID }

// ─── Processing functions ─────────────────────────────────────────────────────

func doubleValue(n Number) (Number, error) {
	return Number{ID: n.ID, Value: n.Value * 2}, nil
}

func incrementValue(n Number) (Number, error) {
	return Number{ID: n.ID, Value: n.Value + 1}, nil
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	cfg := pipeline.DefaultConfigWithSlog(logger)

	// ── Build the pipeline ────────────────────────────────────────────────────
	//
	//   [feeder] ──▶ [double] ──▶ [increment] ──▶ results
	//
	pb := pipeline.NewPipelineBuilder(cfg)

	// Stage 1: double each value (source stage — fed externally).
	pipeline.AddStage(pb, "double", 2, doubleValue)

	// Stage 2: increment each value, chained after "double".
	pipeline.AddStageAfter(pb, "increment", 2, incrementValue, "double")

	// ── Validate before building (optional but recommended) ───────────────────
	if vr := pb.Validate(); !vr.IsValid {
		fmt.Fprintln(os.Stderr, vr.String())
		os.Exit(1)
	}

	// ── Prepare input ─────────────────────────────────────────────────────────
	input := []int{1, 2, 3, 4, 5}
	feeder := make(chan Number, len(input))
	for _, v := range input {
		feeder <- Number{ID: fmt.Sprintf("num-%d", v), Value: v}
	}
	close(feeder)

	// ── Start the pipeline ────────────────────────────────────────────────────
	handle, err := pb.Build(
		context.Background(),
		map[string]any{
			// "double" is the source stage; supply its input channel here.
			"double": (<-chan Number)(feeder),
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Build error: %v\n", err)
		os.Exit(1)
	}

	// ── Collect results ───────────────────────────────────────────────────────
	results, err := pipeline.CollectAll[Number](handle.Results(), "increment")
	if err != nil {
		fmt.Fprintf(os.Stderr, "CollectAll error: %v\n", err)
		os.Exit(1)
	}

	// ── Print ─────────────────────────────────────────────────────────────────
	fmt.Printf("\nResults (%d items):\n", len(results))
	fmt.Println("──────────────────────────────")
	fmt.Printf("%-12s  %-8s  %-8s  %-8s\n", "ID", "input", "×2", "×2+1")
	fmt.Println("──────────────────────────────")
	for i, r := range results {
		original := input[i]
		fmt.Printf("%-12s  %-8d  %-8d  %-8d\n",
			r.ID, original, original*2, r.Value)
	}
	fmt.Println("──────────────────────────────")

	handle.Wait()
	fmt.Println("\nPipeline finished.")
}
