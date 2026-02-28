package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	"pipeline"
)

// ─────────────────────────────────────────────
// Domain types
// ─────────────────────────────────────────────

// Order represents a simple purchase order. It satisfies pipeline.Identifier.
type Order struct {
	ID       string
	Product  string
	Quantity int
	Price    float64
}

func (o Order) GetID() string { return o.ID }

// EnrichedOrder extends Order with a computed Total field.
type EnrichedOrder struct {
	ID       string
	Product  string
	Quantity int
	Price    float64
	Total    float64
}

func (e EnrichedOrder) GetID() string { return e.ID }

// TaggedOrder extends Order with a routing Category field.
type TaggedOrder struct {
	Order
	Category string
}

func (t TaggedOrder) GetID() string { return t.ID }

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func printSection(title string) {
	fmt.Printf("\n%s\n%s\n", title, "═══════════════════════════════════════")
}

func printJSON(label string, v any) {
	b, _ := json.MarshalIndent(v, "  ", "  ")
	fmt.Printf("  %s:\n  %s\n", label, string(b))
}

func sendOrders(orders []Order) <-chan Order {
	ch := make(chan Order, len(orders))
	for _, o := range orders {
		ch <- o
	}
	close(ch)
	return ch
}

// ─────────────────────────────────────────────
// Demo 1 – Two-stage processing pipeline
// ─────────────────────────────────────────────

func demoStage(cfg *pipeline.Config) {
	printSection("1. STAGE – Two-stage processing pipeline")

	source := sendOrders([]Order{
		{ID: "ord-1", Product: "Widget", Quantity: 3, Price: 9.99},
		{ID: "ord-2", Product: "Gadget", Quantity: 1, Price: 49.99},
		{ID: "ord-3", Product: "Doohickey", Quantity: 7, Price: 4.50},
		{ID: "ord-4", Product: "Thingamajig", Quantity: 2, Price: 19.99},
		{ID: "ord-5", Product: "Whatchamacallit", Quantity: 10, Price: 1.25},
	})

	// Stage 1 – enrich orders with a computed Total
	enriched, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"enrich",
		2,
		func(o Order) (EnrichedOrder, error) {
			return EnrichedOrder{
				ID:       o.ID,
				Product:  o.Product,
				Quantity: o.Quantity,
				Price:    o.Price,
				Total:    float64(o.Quantity) * o.Price,
			}, nil
		},
		source,
	))
	if err != nil {
		fmt.Println("  StartStage (enrich) error:", err)
		return
	}

	// Stage 2 – format enriched orders as a human-readable StringItem
	formatted, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"format",
		2,
		func(e EnrichedOrder) (pipeline.StringItem, error) {
			line := fmt.Sprintf("[%s] %s x%d @ $%.2f = $%.2f",
				e.ID, e.Product, e.Quantity, e.Price, e.Total)
			return pipeline.NewStringItemWithID(e.ID, line), nil
		},
		enriched,
	))
	if err != nil {
		fmt.Println("  StartStage (format) error:", err)
		return
	}

	fmt.Println("  Results:")
	for item := range formatted {
		fmt.Println("  »", item.Value)
	}
}

// ─────────────────────────────────────────────
// Demo 2 – Stage with error propagation
// ─────────────────────────────────────────────

func demoStageWithErr() {
	printSection("2. STAGE WITH ERROR – error channel propagation")

	source := make(chan Order, 4)
	source <- Order{ID: "ok-1", Product: "Alpha", Quantity: 2, Price: 5.00}
	source <- Order{ID: "bad-1", Product: "", Quantity: 0, Price: -1}
	source <- Order{ID: "ok-2", Product: "Beta", Quantity: 1, Price: 3.00}
	source <- Order{ID: "bad-2", Product: "", Quantity: 0, Price: -1}
	close(source)

	errCh := make(chan error, 10)

	out := pipeline.StartStageWithErr(
		"validate",
		2,
		func(o Order) (string, error) {
			if o.Product == "" || o.Price < 0 {
				return "", fmt.Errorf("invalid order %q: empty product or negative price", o.ID)
			}
			return fmt.Sprintf("[%s] %s – valid", o.ID, o.Product), nil
		},
		errCh,
		source,
	)

	// Drain results, then close the error channel so the range below terminates
	go func() {
		for line := range out {
			fmt.Println("  ✓", line)
		}
		close(errCh)
	}()

	fmt.Println("  Errors:")
	for e := range errCh {
		fmt.Println("  ✗", e)
	}
}

// ─────────────────────────────────────────────
// Demo 3 – ConditionalRoute
// ─────────────────────────────────────────────

func demoConditionalRoute() {
	printSection("3. CONDITIONAL ROUTE – cheap vs. expensive orders")

	source := make(chan Order, 6)
	source <- Order{ID: "a", Product: "Pen", Price: 1.50}
	source <- Order{ID: "b", Product: "Laptop", Price: 999.00}
	source <- Order{ID: "c", Product: "Notebook", Price: 4.00}
	source <- Order{ID: "d", Product: "Monitor", Price: 350.00}
	source <- Order{ID: "e", Product: "Eraser", Price: 0.50}
	source <- Order{ID: "f", Product: "Keyboard", Price: 120.00}
	close(source)

	expensive, cheap := pipeline.ConditionalRoute(
		source,
		func(o Order) bool { return o.Price >= 100.00 },
		3, 3,
	)

	fmt.Println("  Expensive (>= $100):")
	for o := range expensive {
		fmt.Printf("    [%s] %-15s $%.2f\n", o.ID, o.Product, o.Price)
	}
	fmt.Println("  Cheap (< $100):")
	for o := range cheap {
		fmt.Printf("    [%s] %-15s $%.2f\n", o.ID, o.Product, o.Price)
	}
}

// ─────────────────────────────────────────────
// Demo 4 – RouteByKey
// ─────────────────────────────────────────────

func demoRouteByKey() {
	printSection("4. ROUTE BY KEY – routing by product category")

	source := make(chan TaggedOrder, 5)
	source <- TaggedOrder{Order: Order{ID: "1", Product: "Apple"}, Category: "food"}
	source <- TaggedOrder{Order: Order{ID: "2", Product: "Shirt"}, Category: "clothing"}
	source <- TaggedOrder{Order: Order{ID: "3", Product: "Banana"}, Category: "food"}
	source <- TaggedOrder{Order: Order{ID: "4", Product: "Jeans"}, Category: "clothing"}
	source <- TaggedOrder{Order: Order{ID: "5", Product: "Rice"}, Category: "food"}
	close(source)

	routes := pipeline.RouteByKey(
		source,
		3,
		func(o TaggedOrder) string { return o.Category },
		"food", "clothing",
	)

	fmt.Println("  Food orders:")
	for o := range routes["food"] {
		fmt.Printf("    [%s] %s\n", o.ID, o.Product)
	}
	fmt.Println("  Clothing orders:")
	for o := range routes["clothing"] {
		fmt.Printf("    [%s] %s\n", o.ID, o.Product)
	}
}

// ─────────────────────────────────────────────
// Demo 5 – RouteByPredicate
// ─────────────────────────────────────────────

func demoRouteByPredicate() {
	printSection("5. ROUTE BY PREDICATE – three-tier number prioritisation")

	source := make(chan int, 9)
	for i := 1; i <= 9; i++ {
		source <- i
	}
	close(source)

	// high: > 6  |  medium: 4–6  |  low: < 4
	tiers := pipeline.RouteByPredicate(
		source,
		[]int{3, 3, 3},
		func(n int) bool { return n > 6 },
		func(n int) bool { return n >= 4 && n <= 6 },
		func(n int) bool { return n < 4 },
	)

	labels := []string{"High   (>6) ", "Medium (4-6)", "Low    (<4) "}
	for i, label := range labels {
		fmt.Printf("  %s: ", label)
		for n := range tiers[i] {
			fmt.Printf("%d ", n)
		}
		fmt.Println()
	}
}

// ─────────────────────────────────────────────
// Demo 6 – Fork
// ─────────────────────────────────────────────

func demoFork() {
	printSection("6. FORK – split a mixed channel into int / string lanes")

	source := make(chan any, 6)
	source <- 10
	source <- "hello"
	source <- 20
	source <- "world"
	source <- 30
	source <- "!"
	close(source)

	ints, strs := pipeline.Fork[int, string](source, 3, 3)

	fmt.Print("  Ints:    ")
	for n := range ints {
		fmt.Printf("%d ", n)
	}
	fmt.Println()

	fmt.Print("  Strings: ")
	for s := range strs {
		fmt.Printf("%q ", s)
	}
	fmt.Println()
}

// ─────────────────────────────────────────────
// Demo 7 – MultiTypeRoute
// ─────────────────────────────────────────────

func demoMultiTypeRoute() {
	printSection("7. MULTI-TYPE ROUTE – dispatch by runtime type")

	source := make(chan any, 6)
	source <- 42
	source <- "pipeline"
	source <- 3.14
	source <- 100
	source <- "library"
	source <- 2.71
	close(source)

	tInt := reflect.TypeFor[int]()
	tStr := reflect.TypeFor[string]()
	tF64 := reflect.TypeFor[float64]()

	routes := pipeline.MultiTypeRoute(source, 3, tInt, tStr, tF64)

	fmt.Print("  Ints:    ")
	for v := range routes[tInt] {
		fmt.Printf("%v ", v)
	}
	fmt.Println()

	fmt.Print("  Strings: ")
	for v := range routes[tStr] {
		fmt.Printf("%q ", v)
	}
	fmt.Println()

	fmt.Print("  Float64: ")
	for v := range routes[tF64] {
		fmt.Printf("%.2f ", v)
	}
	fmt.Println()
}

// ─────────────────────────────────────────────
// Demo 8 – Merge
// ─────────────────────────────────────────────

func demoMerge() {
	printSection("8. MERGE – fan-in two channels into one")

	ch1 := make(chan int, 3)
	ch2 := make(chan int, 3)
	ch1 <- 1
	ch1 <- 3
	ch1 <- 5
	ch2 <- 2
	ch2 <- 4
	ch2 <- 6
	close(ch1)
	close(ch2)

	merged := pipeline.Merge(ch1, ch2)

	collected := make([]int, 0, 6)
	for v := range merged {
		collected = append(collected, v)
	}
	fmt.Printf("  Collected %d items (order may vary): %v\n", len(collected), collected)
}

// ─────────────────────────────────────────────
// Demo 9 – Result type
// ─────────────────────────────────────────────

func demoResult() {
	printSection("9. RESULT – success / warning / error handling")

	handle := func(r pipeline.Result[string, string, error]) {
		pipeline.Manage(
			r,
			func(s string) pipeline.StringItem {
				fmt.Printf("  ✓ Success : %s\n", s)
				return pipeline.NewStringItem(s)
			},
			func(w string) pipeline.StringItem {
				fmt.Printf("  ⚠ Warning : %s\n", w)
				return pipeline.NewStringItem(w)
			},
			func(e error) pipeline.StringItem {
				fmt.Printf("  ✗ Error   : %s\n", e)
				return pipeline.NewStringItem(e.Error())
			},
		)
	}

	handle(pipeline.Success[string, string, error]("Order processed successfully"))
	handle(pipeline.Warning[string, string, error]("Order accepted", "stock running low"))
	handle(pipeline.Error[string, string, error]("Order rejected", errors.New("payment declined")))
}

// ─────────────────────────────────────────────
// Demo 10 – ObserverFunc
// ─────────────────────────────────────────────

func demoObserver() {
	printSection("10. OBSERVER – custom ObserverFunc on a stage")

	var eventCount int
	observer := pipeline.ObserverFunc(func(_ context.Context, event pipeline.Eventful) {
		eventCount++
		fmt.Printf("  [EVENT] type=%-30s id=%s\n", event.GetType(), event.GetID())
	})

	cfg := pipeline.DefaultConfig().WithObserver(observer)

	source := sendOrders([]Order{
		{ID: "x1", Product: "Alpha", Quantity: 1, Price: 10},
		{ID: "x2", Product: "Beta", Quantity: 2, Price: 20},
		{ID: "x3", Product: "Gamma", Quantity: 3, Price: 30},
	})

	out, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"observed-stage",
		1,
		func(o Order) (pipeline.StringItem, error) {
			return pipeline.NewStringItemWithID(o.ID, o.Product+" processed"), nil
		},
		source,
	))
	if err != nil {
		fmt.Println("  error:", err)
		return
	}

	for range out {
	}
	fmt.Printf("  Total events observed: %d\n", eventCount)
}

// ─────────────────────────────────────────────
// Demo 11 – MetricsCollector
// ─────────────────────────────────────────────

func demoMetrics() {
	printSection("11. METRICS – collecting stage execution metrics")

	cfg := pipeline.DefaultConfig()
	collector := pipeline.NewMetricsCollector(cfg)
	defer collector.Close()

	// Wire the collector as the observer AFTER creating it so it receives events
	cfg = cfg.WithObserver(collector)

	source := make(chan Order, 6)
	for i := 1; i <= 6; i++ {
		source <- Order{
			ID:       fmt.Sprintf("m-%d", i),
			Product:  fmt.Sprintf("Product-%d", i),
			Quantity: i,
			Price:    float64(i) * 2.50,
		}
	}
	close(source)

	out, err := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"metrics-stage",
		3,
		func(o Order) (pipeline.StringItem, error) {
			if o.Quantity > 5 {
				return pipeline.StringItem{}, fmt.Errorf(
					"quantity %d exceeds limit for order %s", o.Quantity, o.ID)
			}
			time.Sleep(5 * time.Millisecond) // simulate work
			return pipeline.NewStringItemWithID(o.ID, fmt.Sprintf("[%s] OK", o.ID)), nil
		},
		source,
	))
	if err != nil {
		fmt.Println("  error:", err)
		return
	}

	for item := range out {
		fmt.Println("  »", item.Value)
	}

	// Allow the collector's background aggregation loop to catch up
	time.Sleep(200 * time.Millisecond)

	processed, failed := collector.GetRealtimeTotals()
	fmt.Printf("  Real-time totals → processed: %d, failed: %d\n", processed, failed)

	metrics := collector.GetMetrics()
	printJSON("Execution metrics", metrics)
}

// ─────────────────────────────────────────────
// Demo 12 – MultiObserver
// ─────────────────────────────────────────────

func demoMultiObserver() {
	printSection("12. MULTI-OBSERVER – broadcast events to two observers")

	var obs1Count, obs2Count int

	obs1 := pipeline.ObserverFunc(func(_ context.Context, _ pipeline.Eventful) { obs1Count++ })
	obs2 := pipeline.ObserverFunc(func(_ context.Context, _ pipeline.Eventful) { obs2Count++ })
	multi := pipeline.NewMultiObserver(obs1, obs2)

	cfg := pipeline.DefaultConfig().WithObserver(multi)

	source := sendOrders([]Order{
		{ID: "mo-1", Product: "Item-A", Quantity: 1, Price: 5},
		{ID: "mo-2", Product: "Item-B", Quantity: 2, Price: 10},
	})

	out, _ := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"multi-obs-stage",
		1,
		func(o Order) (pipeline.StringItem, error) {
			return pipeline.NewStringItemWithID(o.ID, o.Product+" done"), nil
		},
		source,
	))

	for range out {
	}

	fmt.Printf("  Observer-1 received %d events\n", obs1Count)
	fmt.Printf("  Observer-2 received %d events (same as observer-1)\n", obs2Count)
}

// ─────────────────────────────────────────────
// Demo 13 – Built-in logger helpers
// ─────────────────────────────────────────────

func demoLoggers() {
	printSection("13. LOGGERS – built-in logger constructors")

	fmt.Println("  Text logger (stdout):")
	textLogger := pipeline.DefaultTextLogger()
	pipeline.NewSlogLogger(textLogger).Info("text-logger ready", "component", "demo")

	fmt.Println("  JSON logger (stdout):")
	jsonLogger := pipeline.DefaultJSONLogger()
	pipeline.NewSlogLogger(jsonLogger).Info("json-logger ready", "component", "demo")

	fmt.Println("  Debug JSON logger (stdout, includes DEBUG level):")
	debugLogger := pipeline.DefaultDebugJSONLogger()
	pipeline.NewSlogLogger(debugLogger).Debug("debug message visible", "level", "debug")

	fmt.Println("  Multi-destination logger (stdout + stderr):")
	multiLogger := pipeline.NewMultiLogger(false, slog.LevelInfo, os.Stdout, os.Stderr)
	pipeline.NewSlogLogger(multiLogger).Info("written to both stdout and stderr")

	fmt.Println("  LoggerObserver (routes pipeline events to slog):")
	loggerObs := pipeline.NewLoggerObserver(pipeline.NewSlogLogger(pipeline.DefaultTextLogger()))
	cfg := pipeline.DefaultConfig().WithObserver(loggerObs)
	source := sendOrders([]Order{
		{ID: "lg-1", Product: "Alpha", Quantity: 1, Price: 1},
	})
	out, _ := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"logger-obs-stage",
		1,
		func(o Order) (pipeline.StringItem, error) {
			return pipeline.NewStringItemWithID(o.ID, "done"), nil
		},
		source,
	))
	for range out {
	}
}

// ─────────────────────────────────────────────
// Demo 14 – Context cancellation
// ─────────────────────────────────────────────

func demoContextCancellation() {
	printSection("14. CONTEXT CANCELLATION – stopping a stage early")

	ctx, cancel := context.WithCancel(context.Background())
	cfg := pipeline.DefaultConfig().WithContext(ctx)

	// Endless source; closes itself when the context is done
	source := make(chan Order)
	go func() {
		defer close(source)
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case source <- Order{
				ID:       fmt.Sprintf("ctx-%d", i),
				Product:  "Endless",
				Quantity: 1,
				Price:    1.00,
			}:
				i++
			}
		}
	}()

	out, _ := pipeline.StartStage(pipeline.NewStageConfig(
		cfg,
		"cancellable-stage",
		2,
		func(o Order) (pipeline.StringItem, error) {
			time.Sleep(20 * time.Millisecond) // simulate work
			return pipeline.NewStringItemWithID(o.ID, o.ID), nil
		},
		source,
	))

	// Cancel after a short window
	time.AfterFunc(120*time.Millisecond, cancel)

	count := 0
	for range out {
		count++
	}
	fmt.Printf("  Processed %d items before cancellation\n", count)
}

// ─────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────

func main() {
	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║         PIPELINE LIBRARY DEMO            ║")
	fmt.Println("╚══════════════════════════════════════════╝")

	cfg := pipeline.DefaultConfig()

	demoStage(cfg)
	demoStageWithErr()
	demoConditionalRoute()
	demoRouteByKey()
	demoRouteByPredicate()
	demoFork()
	demoMultiTypeRoute()
	demoMerge()
	demoResult()
	demoObserver()
	demoMetrics()
	demoMultiObserver()
	demoLoggers()
	demoContextCancellation()

	printSection("Done")
}
