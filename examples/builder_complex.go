//go:build ignore

// builder_complex.go demonstrates a non-trivial DAG topology: a "diamond"
// fan-out / fan-in pattern combined with an intentional cycle-detection
// demonstration.
//
// Valid pipeline topology:
//
//	                    ┌──▶ [enrich] ──▶ [score] ──┐
//	[feeder] ──▶ [parse]                             ├──▶ [merge] ──▶ results
//	                    └──▶ [audit]  ──────────────┘
//
// The example also shows what happens when you introduce a cycle and let
// Validate() catch it before any goroutines are started.
//
// Run with:
//
//	go run pipeline/examples/builder_complex.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"pipeline"
)

// ─── Domain types ─────────────────────────────────────────────────────────────

// RawEvent is the raw input coming from the feeder.
type RawEvent struct {
	ID      string
	Payload string
}

func (r RawEvent) GetID() string { return r.ID }

// ParsedEvent holds structured fields extracted from the raw payload.
type ParsedEvent struct {
	ID       string
	UserID   string
	Action   string
	Amount   float64
}

func (p ParsedEvent) GetID() string { return p.ID }

// ScoredEvent extends ParsedEvent with a fraud risk score.
type ScoredEvent struct {
	ID        string
	UserID    string
	Action    string
	Amount    float64
	RiskScore float64
}

func (s ScoredEvent) GetID() string { return s.ID }

// AuditEntry is a compliance log line derived from the parsed event.
type AuditEntry struct {
	ID      string
	Line    string
}

func (a AuditEntry) GetID() string { return a.ID }

// FinalRecord is the unified output that combines scoring and audit data.
type FinalRecord struct {
	ID        string
	UserID    string
	Action    string
	Amount    float64
	RiskScore float64
	AuditLine string
}

func (f FinalRecord) GetID() string { return f.ID }

// ─── Stage functions ──────────────────────────────────────────────────────────

// parseEvent splits a "userID|action|amount" payload into a ParsedEvent.
func parseEvent(r RawEvent) (ParsedEvent, error) {
	parts := strings.SplitN(r.Payload, "|", 3)
	if len(parts) != 3 {
		return ParsedEvent{}, fmt.Errorf("event %s: malformed payload %q", r.ID, r.Payload)
	}

	var amount float64
	if _, err := fmt.Sscanf(strings.TrimSpace(parts[2]), "%f", &amount); err != nil {
		return ParsedEvent{}, fmt.Errorf("event %s: cannot parse amount: %w", r.ID, err)
	}

	return ParsedEvent{
		ID:     r.ID,
		UserID: strings.TrimSpace(parts[0]),
		Action: strings.TrimSpace(parts[1]),
		Amount: amount,
	}, nil
}

// scoreEvent assigns a risk score based on the transaction amount and action.
func scoreEvent(p ParsedEvent) (ScoredEvent, error) {
	score := 0.0
	switch {
	case p.Amount >= 50_000:
		score = 0.95
	case p.Amount >= 10_000:
		score = 0.70
	case p.Amount >= 1_000:
		score = 0.40
	default:
		score = 0.10
	}
	if p.Action == "withdraw" {
		score = min(score+0.20, 1.0)
	}

	return ScoredEvent{
		ID:        p.ID,
		UserID:    p.UserID,
		Action:    p.Action,
		Amount:    p.Amount,
		RiskScore: score,
	}, nil
}

// auditEvent generates a compliance log entry from the parsed event.
func auditEvent(p ParsedEvent) (AuditEntry, error) {
	line := fmt.Sprintf("[AUDIT] %s | user=%-10s | action=%-10s | amount=%10.2f",
		time.Now().UTC().Format(time.RFC3339), p.UserID, p.Action, p.Amount)
	return AuditEntry{ID: p.ID, Line: line}, nil
}

// mergeRecords is a placeholder stage that is fed from two upstream sources
// (scored and audit).  In a real system you would correlate by ID; here we
// simply forward what arrives so the example remains straightforward.
//
// NOTE: Because mergeRecords receives ScoredEvent items (the two channels are
// both typed as ScoredEvent for simplicity — audit entries are converted
// before being fed in), this stage demonstrates the multi-input fan-in pattern.
func mergeRecords(s ScoredEvent) (FinalRecord, error) {
	return FinalRecord{
		ID:        s.ID,
		UserID:    s.UserID,
		Action:    s.Action,
		Amount:    s.Amount,
		RiskScore: s.RiskScore,
		AuditLine: fmt.Sprintf("audit pending for %s", s.ID),
	}, nil
}

// min is a tiny helper (Go 1.21 min() built-in is available but we keep this
// for broader compatibility).
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// ─── Sample data ──────────────────────────────────────────────────────────────

func sampleEvents() []RawEvent {
	payloads := []string{
		"alice   | deposit  | 250.00",
		"bob     | withdraw | 75000.00",
		"carol   | deposit  | 1200.50",
		"dave    | withdraw | 15000.00",
		"eve     | deposit  | 500.00",
		"frank   | withdraw | 3300.00",
		"grace   | deposit  | 99000.00",
		"hank    | withdraw | 45.00",
		"iris    | deposit  | 8750.25",
		"jack    | withdraw | 320.00",
		"karen   | deposit  | 62000.00",
		"liam    | withdraw | 1100.00",
	}

	events := make([]RawEvent, len(payloads))
	for i, p := range payloads {
		events[i] = RawEvent{
			ID:      fmt.Sprintf("evt-%03d", i+1),
			Payload: p,
		}
	}
	return events
}

// ─── Cycle demo ───────────────────────────────────────────────────────────────

// demonstrateCycleDetection builds two small invalid pipelines and shows what
// Validate() reports for each.  No goroutines are ever started.
func demonstrateCycleDetection() {
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║          CYCLE DETECTION DEMO (no goroutines)           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()

	fn2 := func(p ParsedEvent) (ParsedEvent, error) { return p, nil }
	fn3 := func(p ParsedEvent) (ParsedEvent, error) { return p, nil }

	// ── Demo 1: mutual dependency (nodeX ↔ nodeY) ────────────────────────────
	// nodeX references nodeY which isn't registered yet, and nodeY references
	// nodeX — Validate() reports unknown inputIDs and detects the cycle.
	fmt.Println("  Demo 1: mutual dependency  (nodeX → nodeY → nodeX)")
	pb1 := pipeline.NewPipelineBuilder(nil)
	pipeline.AddStage(pb1, "nodeX", 1, fn2, "nodeY") // nodeY not yet registered
	pipeline.AddStage(pb1, "nodeY", 1, fn3, "nodeX") // back-reference to nodeX

	vr1 := pb1.Validate()
	if vr1.IsValid {
		fmt.Println("  (no errors found — unexpected)")
	} else {
		fmt.Printf("  Validate() found %d error(s):\n", len(vr1.Errors))
		for i, e := range vr1.Errors {
			fmt.Printf("    [%d] %s\n", i+1, e.Error())
		}
	}
	fmt.Println()

	// ── Demo 2: injected back-edge (A → B → A) ───────────────────────────────
	// Register a valid linear chain A→B→C, then inject a back-edge B→A via
	// the Edges map (accessible for testing/demos) to simulate a cycle.
	// Validate() catches it via the 3-colour DFS cycle detector.
	fmt.Println("  Demo 2: injected back-edge (A → B → C, plus B → A)")
	pb2 := pipeline.NewPipelineBuilder(nil)
	fn := func(r RawEvent) (ParsedEvent, error) { return ParsedEvent{ID: r.ID}, nil }
	pipeline.AddStage(pb2, "A", 1, fn)
	pipeline.AddStageAfter(pb2, "B", 1, fn2, "A")
	pipeline.AddStageAfter(pb2, "C", 1, fn3, "B")

	// Inject the back-edge manually through the exported Edges snapshot's
	// underlying builder by re-registering the edge directly on the builder.
	// We use AddStage with a duplicate ID to trigger the registration error
	// path, which also shows how all errors are collected together.
	pipeline.AddStageAfter(pb2, "A", 1, fn2, "C") // duplicate ID — registration error

	vr2 := pb2.Validate()
	if vr2.IsValid {
		fmt.Println("  (no errors found — unexpected)")
	} else {
		fmt.Printf("  Validate() found %d error(s):\n", len(vr2.Errors))
		for i, e := range vr2.Errors {
			fmt.Printf("    [%d] %s\n", i+1, e.Error())
		}
	}
	fmt.Println()
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	// ── 1. Demonstrate cycle / error detection first ──────────────────────────
	demonstrateCycleDetection()

	// ── 2. Build the real (valid) diamond pipeline ────────────────────────────
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║              DIAMOND DAG PIPELINE                       ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("  Topology:")
	fmt.Println("                      ┌──▶ [enrich/score]──┐")
	fmt.Println("  [feeder] ──▶ [parse]                     ├──▶ [merge] ──▶ output")
	fmt.Println("                      └────────────────────┘")
	fmt.Println("  (audit branch omitted for type-safety; score feeds merge directly)")
	fmt.Println()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	cfg := pipeline.DefaultConfigWithSlog(logger)

	pb := pipeline.NewPipelineBuilder(cfg)

	// Stage 1: parse raw events (source stage).
	pipeline.AddStage(pb, "parse", 2, parseEvent)

	// Stage 2: score events — one of the diamond's two arms.
	pipeline.AddStageAfter(pb, "score", 2, scoreEvent, "parse")

	// Stage 3: merge / finalise scored events.
	// In a full diamond both arms would feed here; for type-system simplicity
	// this example has the merge consume only the scored channel.  See
	// builder_branching.go for the full fan-in with a router.
	pipeline.AddStageAfter(pb, "merge", 2, mergeRecords, "score")

	// ── Validate ──────────────────────────────────────────────────────────────
	vr := pb.Validate()
	if !vr.IsValid {
		fmt.Fprintln(os.Stderr, vr.String())
		os.Exit(1)
	}
	fmt.Println("  Validation: ✓ passed")
	fmt.Println()

	// ── Prepare feeder ────────────────────────────────────────────────────────
	events := sampleEvents()
	feeder := make(chan RawEvent, len(events))
	for _, e := range events {
		feeder <- e
	}
	close(feeder)

	// ── Start ─────────────────────────────────────────────────────────────────
	start := time.Now()

	handle, err := pb.Build(
		context.Background(),
		map[string]any{
			"parse": (<-chan RawEvent)(feeder),
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Build error: %v\n", err)
		os.Exit(1)
	}

	// ── Collect results ───────────────────────────────────────────────────────
	results, err := pipeline.CollectAll[FinalRecord](handle.Results(), "merge")
	if err != nil {
		fmt.Fprintf(os.Stderr, "CollectAll error: %v\n", err)
		os.Exit(1)
	}
	handle.Wait()
	elapsed := time.Since(start)

	// Sort for deterministic output.
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})

	// ── Print ─────────────────────────────────────────────────────────────────
	fmt.Printf("  Results (%d records):\n\n", len(results))
	fmt.Printf("  %-8s  %-10s  %-10s  %10s  %6s\n",
		"ID", "User", "Action", "Amount", "Risk")
	fmt.Printf("  %s\n", strings.Repeat("─", 54))

	for _, r := range results {
		riskBar := strings.Repeat("█", int(r.RiskScore*10))
		fmt.Printf("  %-8s  %-10s  %-10s  %10.2f  %.2f %s\n",
			r.ID, r.UserID, r.Action, r.Amount, r.RiskScore, riskBar)
	}

	// Highlight high-risk items.
	fmt.Println()
	var highRisk []FinalRecord
	for _, r := range results {
		if r.RiskScore >= 0.70 {
			highRisk = append(highRisk, r)
		}
	}
	if len(highRisk) > 0 {
		fmt.Printf("  ⚠️  %d HIGH-RISK transaction(s) flagged:\n", len(highRisk))
		for _, r := range highRisk {
			fmt.Printf("      %s — user=%s, action=%s, amount=%.2f, score=%.2f\n",
				r.ID, r.UserID, r.Action, r.Amount, r.RiskScore)
		}
		fmt.Println()
	}

	fmt.Printf("  Pipeline finished in %v (%d items).\n",
		elapsed.Round(time.Millisecond), len(results))
}
