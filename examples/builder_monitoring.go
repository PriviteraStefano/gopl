//go:build ignore

// builder_monitoring.go demonstrates how to wire the existing MetricsCollector
// and LoggerObserver into a PipelineBuilder pipeline so that every stage event
// is automatically captured without any extra instrumentation in business logic.
//
// The pipeline models a simple ETL flow:
//
//	[feeder] ──▶ [parse] ──▶ [enrich] ──▶ [persist]
//
// After the pipeline finishes, aggregated metrics are printed to stdout.
//
// Run with:
//
//	go run pipeline/examples/builder_monitoring.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"gopl"
)

// ─── Domain types ─────────────────────────────────────────────────────────────

// RawRecord represents a raw CSV-like string coming off the wire.
type RawRecord struct {
	ID   string
	Line string
}

func (r RawRecord) GetID() string { return r.ID }

// ParsedRecord is the result of splitting the raw line into fields.
type ParsedRecord struct {
	ID     string
	Name   string
	Amount float64
}

func (p ParsedRecord) GetID() string { return p.ID }

// EnrichedRecord adds a derived risk tier based on the amount.
type EnrichedRecord struct {
	ID       string
	Name     string
	Amount   float64
	RiskTier string
}

func (e EnrichedRecord) GetID() string { return e.ID }

// PersistedRecord is the final write-confirmation message.
type PersistedRecord struct {
	ID      string
	Summary string
}

func (p PersistedRecord) GetID() string { return p.ID }

// ─── Stage functions ──────────────────────────────────────────────────────────

// parse splits a raw "name,amount" line into a ParsedRecord.
func parse(r RawRecord) (ParsedRecord, error) {
	// Simulate occasional parse latency.
	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)

	parts := strings.SplitN(r.Line, ",", 2)
	if len(parts) != 2 {
		return ParsedRecord{}, fmt.Errorf("record %s: malformed line %q", r.ID, r.Line)
	}

	var amount float64
	if _, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%f", &amount); err != nil {
		return ParsedRecord{}, fmt.Errorf("record %s: cannot parse amount: %w", r.ID, err)
	}

	return ParsedRecord{
		ID:     r.ID,
		Name:   strings.TrimSpace(parts[0]),
		Amount: amount,
	}, nil
}

// enrich assigns a risk tier and simulates a lightweight enrichment lookup.
func enrich(p ParsedRecord) (EnrichedRecord, error) {
	time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

	tier := "low"
	switch {
	case p.Amount >= 10_000:
		tier = "high"
	case p.Amount >= 1_000:
		tier = "medium"
	}

	return EnrichedRecord{
		ID:       p.ID,
		Name:     p.Name,
		Amount:   p.Amount,
		RiskTier: tier,
	}, nil
}

// persist simulates writing to a database / downstream service.
func persist(e EnrichedRecord) (PersistedRecord, error) {
	time.Sleep(time.Duration(rand.Intn(4)) * time.Millisecond)

	return PersistedRecord{
		ID: e.ID,
		Summary: fmt.Sprintf("stored %-20s | amount: %10.2f | tier: %s",
			e.Name, e.Amount, e.RiskTier),
	}, nil
}

// ─── Sample data ──────────────────────────────────────────────────────────────

func sampleRecords() []RawRecord {
	lines := []string{
		"Alice,   500.00",
		"Bob,  12500.00",
		"Carol,  1750.50",
		"Dave,    99.99",
		"Eve,  45000.00",
		"Frank,  320.00",
		"Grace, 8200.00",
		"Hank,    15.00",
		"Iris,  2100.75",
		"Jack,   880.40",
		"Karen,95000.00",
		"Liam,   210.30",
		"Mia,  3500.00",
		"Noah,   450.00",
		"Olivia,1000.00",
		"Peter, 7890.10",
		"Quinn,  100.00",
		"Rose, 22000.00",
		"Sam,   640.50",
		"Tina, 11000.00",
	}

	records := make([]RawRecord, len(lines))
	for i, l := range lines {
		records[i] = RawRecord{
			ID:   fmt.Sprintf("rec-%03d", i+1),
			Line: l,
		}
	}
	return records
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// ── Observability setup ───────────────────────────────────────────────────

	// Structured logger (text format so output is human-readable in the terminal).
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	logger := gopl.NewSlogLogger(slogLogger)

	// LoggerObserver forwards every pipeline event to the structured logger.
	loggerObserver := gopl.NewLoggerObserver(logger)

	// MetricsCollector gathers counters, timings, and per-stage breakdowns.
	baseCfg := &gopl.Config{
		Logger:   logger,
		Observer: gopl.NoOpObserver{}, // placeholder; replaced below
		Context:  context.Background(),
	}
	metricsCollector := gopl.NewMetricsCollector(baseCfg)
	defer metricsCollector.Close()

	// Combine both observers so every event reaches the logger AND the metrics
	// collector simultaneously.
	multiObs := gopl.NewMultiObserver(loggerObserver, metricsCollector)

	cfg := gopl.DefaultConfig().
		WithLogger(logger).
		WithObserver(multiObs)

	// ── Build ─────────────────────────────────────────────────────────────────
	//
	//   [feeder] ──▶ [parse:2] ──▶ [enrich:4] ──▶ [persist:2] ──▶ results
	//
	pb := gopl.NewPipelineBuilder(cfg)

	gopl.AddStage(pb, "parse", 2, parse)
	gopl.AddStageAfter(pb, "enrich", 4, enrich, "parse")
	gopl.AddStageAfter(pb, "persist", 2, persist, "enrich")

	// ── Validate ──────────────────────────────────────────────────────────────
	if vr := pb.Validate(); !vr.IsValid {
		fmt.Fprintln(os.Stderr, vr.String())
		os.Exit(1)
	}

	// ── Prepare feeder ────────────────────────────────────────────────────────
	records := sampleRecords()
	feeder := make(chan RawRecord, len(records))
	for _, r := range records {
		feeder <- r
	}
	close(feeder)

	// ── Start ─────────────────────────────────────────────────────────────────
	start := time.Now()

	handle, err := pb.Build(
		context.Background(),
		map[string]any{
			"parse": (<-chan RawRecord)(feeder),
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Build error: %v\n", err)
		os.Exit(1)
	}

	// ── Collect results ───────────────────────────────────────────────────────
	results, err := gopl.CollectAll[PersistedRecord](handle.Results(), "persist")
	if err != nil {
		fmt.Fprintf(os.Stderr, "CollectAll error: %v\n", err)
		os.Exit(1)
	}
	handle.Wait()
	elapsed := time.Since(start)

	// Allow metrics collector one final aggregation cycle.
	time.Sleep(200 * time.Millisecond)

	// ── Print results ─────────────────────────────────────────────────────────
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                   PIPELINE RESULTS                         ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n\n")

	for _, r := range results {
		fmt.Printf("  [%s] %s\n", r.ID, r.Summary)
	}

	// ── Print metrics ─────────────────────────────────────────────────────────
	metrics := metricsCollector.GetMetrics()
	processed, failed := metricsCollector.GetRealtimeTotals()

	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                   EXECUTION METRICS                        ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n\n")

	fmt.Printf("  Wall-clock time:   %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Items processed:   %d\n", processed)
	fmt.Printf("  Items failed:      %d\n", failed)
	fmt.Printf("  Results collected: %d\n\n", len(results))

	// Per-stage breakdown.
	stageNames := []string{"parse", "enrich", "persist"}
	fmt.Printf("  %-12s  %8s  %8s  %10s\n", "Stage", "Success", "Failed", "Duration")
	fmt.Printf("  %s\n", strings.Repeat("─", 46))
	for _, name := range stageNames {
		if sm, ok := metrics.StagesMetrics[name]; ok {
			dur := sm.Duration.Round(time.Millisecond)
			fmt.Printf("  %-12s  %8d  %8d  %10v\n",
				name, sm.ProcessedCounter, sm.FailedCounter, dur)
		} else {
			fmt.Printf("  %-12s  %8s  %8s  %10s\n", name, "–", "–", "–")
		}
	}

	fmt.Printf("\nPipeline finished in %v.\n", elapsed.Round(time.Millisecond))
}
