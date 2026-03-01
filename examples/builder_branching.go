//go:build ignore

// builder_branching.go demonstrates how to wire a router into a PipelineBuilder
// pipeline.  Orders are classified as "premium" or "standard" based on their
// total value, and each branch is processed by a dedicated downstream stage.
//
// Run with:
//
//	go run pipeline/examples/builder_branching.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"pipeline"
)

// ─── Domain types ─────────────────────────────────────────────────────────────

// Order is the raw input item.
type Order struct {
	ID       string
	Product  string
	Quantity int
	Price    float64
}

func (o Order) GetID() string { return o.ID }

// EnrichedOrder carries the computed total alongside the original fields.
type EnrichedOrder struct {
	ID       string
	Product  string
	Quantity int
	Price    float64
	Total    float64
}

func (e EnrichedOrder) GetID() string { return e.ID }

// ProcessedOrder is the final output of each branch.
type ProcessedOrder struct {
	ID      string
	Total   float64
	Tier    string
	Message string
}

func (p ProcessedOrder) GetID() string { return p.ID }

// ─── Stage functions ──────────────────────────────────────────────────────────

// enrich computes the order total.
func enrich(o Order) (EnrichedOrder, error) {
	return EnrichedOrder{
		ID:       o.ID,
		Product:  o.Product,
		Quantity: o.Quantity,
		Price:    o.Price,
		Total:    float64(o.Quantity) * o.Price,
	}, nil
}

// processPremium handles high-value orders.
func processPremium(o EnrichedOrder) (ProcessedOrder, error) {
	return ProcessedOrder{
		ID:      o.ID,
		Total:   o.Total,
		Tier:    "premium",
		Message: fmt.Sprintf("PREMIUM: %s — $%.2f (priority fulfilment)", o.Product, o.Total),
	}, nil
}

// processStandard handles regular orders.
func processStandard(o EnrichedOrder) (ProcessedOrder, error) {
	return ProcessedOrder{
		ID:      o.ID,
		Total:   o.Total,
		Tier:    "standard",
		Message: fmt.Sprintf("STANDARD: %s — $%.2f", o.Product, o.Total),
	}, nil
}

// ─── Router function ──────────────────────────────────────────────────────────

const premiumThreshold = 100.0

// splitByTier routes enriched orders into "premium" and "standard" branches.
func splitByTier(in <-chan EnrichedOrder) map[string]<-chan EnrichedOrder {
	premium := make(chan EnrichedOrder, 16)
	standard := make(chan EnrichedOrder, 16)

	go func() {
		defer close(premium)
		defer close(standard)
		for order := range in {
			if order.Total >= premiumThreshold {
				premium <- order
			} else {
				standard <- order
			}
		}
	}()

	return map[string]<-chan EnrichedOrder{
		"premium":  premium,
		"standard": standard,
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	cfg := pipeline.DefaultConfigWithSlog(logger)

	// ── Build the pipeline ────────────────────────────────────────────────────
	//
	//   [feeder] ──▶ [enrich] ──▶ [splitByTier] ──▶ [premium]  ──▶ results
	//                                            └──▶ [standard] ──▶ results
	//
	pb := pipeline.NewPipelineBuilder(cfg)

	// Stage 1: enrich orders (source stage).
	pipeline.AddStage(pb, "enrich", 2, enrich)

	// Router: split enriched orders by tier.
	pipeline.AddRouter(pb, "split", splitByTier, "enrich")

	// Stage 2a: process premium branch.
	pipeline.AddStage(pb, "premium", 2, processPremium,
		pipeline.RouterBranchRef{RouterID: "split", BranchKey: "premium"}.ID())

	// Stage 2b: process standard branch.
	pipeline.AddStage(pb, "standard", 2, processStandard,
		pipeline.RouterBranchRef{RouterID: "split", BranchKey: "standard"}.ID())

	// ── Validate ──────────────────────────────────────────────────────────────
	if vr := pb.Validate(); !vr.IsValid {
		fmt.Fprintln(os.Stderr, vr.String())
		os.Exit(1)
	}

	// ── Prepare input ─────────────────────────────────────────────────────────
	orders := []Order{
		{ID: "ord-001", Product: "Laptop",     Quantity: 1, Price: 999.99},
		{ID: "ord-002", Product: "USB Cable",  Quantity: 3, Price:   9.99},
		{ID: "ord-003", Product: "Monitor",    Quantity: 2, Price: 299.00},
		{ID: "ord-004", Product: "Mouse Pad",  Quantity: 5, Price:   8.50},
		{ID: "ord-005", Product: "Headphones", Quantity: 1, Price: 149.00},
		{ID: "ord-006", Product: "Pen",        Quantity: 10, Price:  1.50},
		{ID: "ord-007", Product: "SSD 1TB",    Quantity: 2, Price: 109.95},
		{ID: "ord-008", Product: "Notebook",   Quantity: 4, Price:   4.99},
	}

	feeder := make(chan Order, len(orders))
	for _, o := range orders {
		feeder <- o
	}
	close(feeder)

	// ── Start ─────────────────────────────────────────────────────────────────
	handle, err := pb.Build(
		context.Background(),
		map[string]any{
			"enrich": (<-chan Order)(feeder),
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Build error: %v\n", err)
		os.Exit(1)
	}

	// ── Collect results from both branches ────────────────────────────────────
	premiumResults, err := pipeline.CollectAll[ProcessedOrder](handle.Results(), "premium")
	if err != nil {
		fmt.Fprintf(os.Stderr, "CollectAll premium error: %v\n", err)
		os.Exit(1)
	}

	standardResults, err := pipeline.CollectAll[ProcessedOrder](handle.Results(), "standard")
	if err != nil {
		fmt.Fprintf(os.Stderr, "CollectAll standard error: %v\n", err)
		os.Exit(1)
	}

	// Sort for deterministic output.
	sort.Slice(premiumResults, func(i, j int) bool {
		return premiumResults[i].ID < premiumResults[j].ID
	})
	sort.Slice(standardResults, func(i, j int) bool {
		return standardResults[i].ID < standardResults[j].ID
	})

	// ── Print ─────────────────────────────────────────────────────────────────
	fmt.Printf("\n🏆 Premium orders (total ≥ $%.2f):\n", premiumThreshold)
	fmt.Println("─────────────────────────────────────────────────────────")
	for _, r := range premiumResults {
		fmt.Printf("  [%s] %s\n", r.ID, r.Message)
	}

	fmt.Printf("\n📦 Standard orders (total < $%.2f):\n", premiumThreshold)
	fmt.Println("─────────────────────────────────────────────────────────")
	for _, r := range standardResults {
		fmt.Printf("  [%s] %s\n", r.ID, r.Message)
	}

	fmt.Printf("\nSummary: %d premium, %d standard (total: %d)\n",
		len(premiumResults), len(standardResults),
		len(premiumResults)+len(standardResults))

	handle.Wait()
	fmt.Println("\nPipeline finished.")
}
