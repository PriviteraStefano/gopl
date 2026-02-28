package pipeline

// import (
// 	"context"
// 	"fmt"
// 	"sync"
// 	"time"
// )

// // Handler is the function type that processes data in a stage
// type Handler func(ctx context.Context, input Identifier) (Identifier, error)

// // StageOption is a functional option for configuring stages
// type StageOption func(*StageConfig)

// // ErrorPolicy defines error handling strategy
// type ErrorPolicy string

// const (
// 	ErrorPolicyFail     ErrorPolicy = "fail"     // stop pipeline on error
// 	ErrorPolicyIgnore   ErrorPolicy = "ignore"   // skip item and continue
// 	ErrorPolicyContinue ErrorPolicy = "continue" // continue processing
// )

// // StageConfig holds configuration for a pipeline stage
// type StageConfig struct {
// 	Name        string
// 	Handler     Handler
// 	Workers     int
// 	BufferSize  int
// 	ErrorPolicy ErrorPolicy
// 	Timeout     time.Duration
// }

// // RouterConfig defines routing rules between stages
// type RouterConfig struct {
// 	Source  string
// 	Routes  map[string]func(Identifier) bool
// 	Default string
// }

// // Pipeline represents a configured data processing pipeline
// type Pipeline struct {
// 	mu        sync.RWMutex
// 	stages    map[string]*StageConfig
// 	routers   map[string]*RouterConfig
// 	stageList []*StageConfig // maintains insertion order
// }

// // PipelineBuilder provides a fluent interface for constructing pipelines
// type PipelineBuilder struct {
// 	stages     map[string]*StageConfig
// 	stageList  []*StageConfig // maintains insertion order
// 	routers    map[string]*RouterConfig
// 	validators []func(*Pipeline) error
// }

// // PipelineResult holds the output of pipeline execution
// type PipelineResult struct {
// 	Data      interface{}
// 	Error     error
// 	StageName string
// }

// // ======== Builder Methods ========

// // NewPipelineBuilder creates a new pipeline builder
// func NewPipelineBuilder() *PipelineBuilder {
// 	return &PipelineBuilder{
// 		stages:    make(map[string]*StageConfig),
// 		stageList: []*StageConfig{},
// 		routers:   make(map[string]*RouterConfig),
// 	}
// }

// // AddStage adds a processing stage to the pipeline
// func (pb *PipelineBuilder) AddStage(name string, handler Handler, opts ...StageOption) *PipelineBuilder {
// 	if _, exists := pb.stages[name]; exists {
// 		panic(fmt.Sprintf("stage %q already exists", name))
// 	}

// 	config := &StageConfig{
// 		Name:        name,
// 		Handler:     handler,
// 		Workers:     1,
// 		BufferSize:  100,
// 		ErrorPolicy: ErrorPolicyFail,
// 		Timeout:     0, // no timeout
// 	}

// 	for _, opt := range opts {
// 		opt(config)
// 	}

// 	pb.stages[name] = config
// 	pb.stageList = append(pb.stageList, config)
// 	return pb
// }

// // WithWorkers sets the number of concurrent workers for a stage
// func WithWorkers(count int) StageOption {
// 	return func(c *StageConfig) {
// 		if count <= 0 {
// 			panic("workers must be greater than 0")
// 		}
// 		c.Workers = count
// 	}
// }

// // WithBufferSize sets the channel buffer size
// func WithBufferSize(size int) StageOption {
// 	return func(c *StageConfig) {
// 		if size < 0 {
// 			panic("buffer size cannot be negative")
// 		}
// 		c.BufferSize = size
// 	}
// }

// // WithErrorPolicy sets error handling behavior
// func WithErrorPolicy(policy ErrorPolicy) StageOption {
// 	return func(c *StageConfig) {
// 		c.ErrorPolicy = policy
// 	}
// }

// // WithTimeout sets a timeout for processing each item
// func WithTimeout(timeout time.Duration) StageOption {
// 	return func(c *StageConfig) {
// 		c.Timeout = timeout
// 	}
// }

// // Route configures routing from one stage to multiple destinations
// func (pb *PipelineBuilder) Route(source string, routes map[string]func(Identifier) bool, defaultRoute string) *PipelineBuilder {
// 	if _, exists := pb.stages[source]; !exists {
// 		panic(fmt.Sprintf("source stage %q does not exist", source))
// 	}

// 	// Validate all route destinations exist
// 	for dest := range routes {
// 		if _, exists := pb.stages[dest]; !exists {
// 			panic(fmt.Sprintf("destination stage %q does not exist", dest))
// 		}
// 	}

// 	// Validate default route if specified
// 	if defaultRoute != "" {
// 		if _, exists := pb.stages[defaultRoute]; !exists {
// 			panic(fmt.Sprintf("default route stage %q does not exist", defaultRoute))
// 		}
// 	}

// 	pb.routers[source] = &RouterConfig{
// 		Source:  source,
// 		Routes:  routes,
// 		Default: defaultRoute,
// 	}
// 	return pb
// }

// // ValidateDAG ensures no circular routing exists
// func (pb *PipelineBuilder) ValidateDAG() *PipelineBuilder {
// 	pb.validators = append(pb.validators, func(p *Pipeline) error {
// 		visited := make(map[string]bool)
// 		rec := make(map[string]bool)

// 		for stageName := range p.stages {
// 			if hasCycle(stageName, p.routers, visited, rec) {
// 				return fmt.Errorf("circular routing detected at stage: %s", stageName)
// 			}
// 		}
// 		return nil
// 	})
// 	return pb
// }

// // ValidateStages ensures all stages have valid handlers
// func (pb *PipelineBuilder) ValidateStages() *PipelineBuilder {
// 	pb.validators = append(pb.validators, func(p *Pipeline) error {
// 		for _, stage := range p.stages {
// 			if stage.Handler == nil {
// 				return fmt.Errorf("stage %q has no handler", stage.Name)
// 			}
// 		}
// 		return nil
// 	})
// 	return pb
// }

// // Build validates and constructs the final pipeline
// func (pb *PipelineBuilder) Build(ctx context.Context) (*Pipeline, error) {
// 	// Create the pipeline
// 	pipeline := &Pipeline{
// 		stages:    pb.stages,
// 		routers:   pb.routers,
// 		stageList: pb.stageList,
// 	}

// 	// Run validators
// 	for _, validator := range pb.validators {
// 		if err := validator(pipeline); err != nil {
// 			return nil, fmt.Errorf("pipeline validation failed: %w", err)
// 		}
// 	}

// 	return pipeline, nil
// }

// // ======== Pipeline Methods ========

// // Run executes the pipeline with the provided data source
// func (p *Pipeline) Run(ctx context.Context, dataSource <-chan Identifier) (<-chan PipelineResult, <-chan error) {
// 	results := make(chan PipelineResult, 100)
// 	errs := make(chan error, 1)

// 	go func() {
// 		defer close(results)
// 		defer close(errs)

// 		p.mu.RLock()
// 		defer p.mu.RUnlock()

// 		// Create channels for each stage
// 		stageChannels := make(map[string]<-chan Identifier)
// 		stageInputs := make(map[string]chan<- Identifier)

// 		// Initialize first stage with data source
// 		if len(p.stageList) == 0 {
// 			errs <- fmt.Errorf("pipeline has no stages")
// 			return
// 		}

// 		firstStageName := p.stageList[0].Name
// 		firstStageInput := make(chan Identifier, p.stages[firstStageName].BufferSize)
// 		stageInputs[firstStageName] = firstStageInput
// 		stageChannels[firstStageName] = p.executeStage(ctx, firstStageName, firstStageInput, results)

// 		// Initialize remaining stages based on routing
// 		for i := 1; i < len(p.stageList); i++ {
// 			stageName := p.stageList[i].Name
// 			stageInput := make(chan Identifier, p.stages[stageName].BufferSize)
// 			stageInputs[stageName] = stageInput
// 			stageChannels[stageName] = p.executeStage(ctx, stageName, stageInput, results)
// 		}

// 		// Feed data to first stage
// 		go func() {
// 			defer close(firstStageInput)
// 			for data := range dataSource {
// 				select {
// 				case <-ctx.Done():
// 					return
// 				case firstStageInput <- data:
// 				}
// 			}
// 		}()

// 		// Wait for all stages to complete
// 		for _, stageName := range p.stageList {
// 			<-stageChannels[stageName.Name]
// 		}
// 	}()

// 	return results, errs
// }

// // executeStage runs a single stage with its workers
// func (p *Pipeline) executeStage(ctx context.Context, stageName string, input <-chan Identifier, results chan<- PipelineResult) <-chan Identifier {
// 	done := make(chan Identifier)
// 	stage := p.stages[stageName]

// 	go func() {
// 		defer close(done)
// 		var wg sync.WaitGroup

// 		// Create worker goroutines
// 		for w := 0; w < stage.Workers; w++ {
// 			wg.Add(1)
// 			go func() {
// 				defer wg.Done()
// 				p.worker(ctx, stageName, stage, input, results)
// 			}()
// 		}

// 		wg.Wait()
// 	}()

// 	return done
// }

// // worker processes items from the input channel
// func (p *Pipeline) worker(ctx context.Context, stageName string, stage *StageConfig, input <-chan Identifier, results chan<- PipelineResult) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case data, ok := <-input:
// 			if !ok {
// 				return
// 			}

// 			// Apply timeout if configured
// 			processCtx := ctx
// 			if stage.Timeout > 0 {
// 				var cancel context.CancelFunc
// 				processCtx, cancel = context.WithTimeout(ctx, stage.Timeout)
// 				defer cancel()
// 			}

// 			// Execute the handler
// 			output, err := stage.Handler(processCtx, data)

// 			// Handle errors based on policy
// 			if err != nil {
// 				switch stage.ErrorPolicy {
// 				case ErrorPolicyFail:
// 					results <- PipelineResult{
// 						Data:      nil,
// 						Error:     err,
// 						StageName: stageName,
// 					}
// 					return
// 				case ErrorPolicyIgnore:
// 					// Skip this item and continue
// 					continue
// 				case ErrorPolicyContinue:
// 					// Send error but continue
// 					results <- PipelineResult{
// 						Data:      data,
// 						Error:     err,
// 						StageName: stageName,
// 					}
// 					continue
// 				}
// 			}

// 			// Route to next stage(s)
// 			p.routeData(ctx, stageName, output, results)
// 		}
// 	}
// }

// // routeData sends data to the appropriate next stage based on routing rules
// func (p *Pipeline) routeData(ctx context.Context, fromStage string, data Identifier, results chan<- PipelineResult) {
// 	// Send successful result
// 	results <- PipelineResult{
// 		Data:      data,
// 		Error:     nil,
// 		StageName: fromStage,
// 	}

// 	// Check if there's routing configured for this stage
// 	router, exists := p.routers[fromStage]
// 	if !exists {
// 		// No routing, this is likely a final stage
// 		return
// 	}

// 	// Find which route this data should take
// 	for routeName, routeFunc := range router.Routes {
// 		if routeFunc(data) {
// 			// TODO: Send to route stage
// 			// This would require maintaining output channels for each stage
// 			_ = routeName
// 			break
// 		}
// 	}

// 	// Use default route if specified
// 	if router.Default != "" {
// 		// TODO: Send to default route stage
// 	}
// }

// // ======== Utility Functions ========

// // hasCycle detects if there's a circular dependency in the routing graph
// func hasCycle(node string, routers map[string]*RouterConfig, visited, rec map[string]bool) bool {
// 	visited[node] = true
// 	rec[node] = true

// 	router, exists := routers[node]
// 	if !exists {
// 		rec[node] = false
// 		return false
// 	}

// 	// Check all routes from this node
// 	for nextNode := range router.Routes {
// 		if !visited[nextNode] {
// 			if hasCycle(nextNode, routers, visited, rec) {
// 				return true
// 			}
// 		} else if rec[nextNode] {
// 			return true
// 		}
// 	}

// 	// Check default route
// 	if router.Default != "" && !visited[router.Default] {
// 		if hasCycle(router.Default, routers, visited, rec) {
// 			return true
// 		}
// 	}

// 	rec[node] = false
// 	return false
// }
