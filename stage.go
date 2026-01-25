package pipeline

import (
	"fmt"
	"strconv"
	"sync"
)

// StartStageWithErr starts a stage with error handling
func StartStageWithErr[I, O any](
	id string,
	workers int,
	fn func(I) (O, error),
	ec chan error, c ...<-chan I,
) <-chan O {
	// Validate input channels
	if len(c) == 0 {
		ec <- fmt.Errorf("no input channels provided")
		return nil
	}
	var ch <-chan I
	if len(c) == 1 {
		ch = c[0]
	} else {
		ch = Merge(c...)
	}

	// Setup stage workers
	out := make(chan O, workers)
	var stageWg sync.WaitGroup
	stageWg.Add(workers)

	// Initialize stage workers
	for i := range workers {
		go func() {
			defer stageWg.Done()
			for item := range ch {
				fmt.Printf("worker %d starts stage %s\n", i, id)
				result, err := fn(item)
				if err != nil {
					ec <- fmt.Errorf("%s stage processing error | %w", id, err)
					continue
				}
				out <- result
			}
		}()
	}

	// Close the output channel when stage workers are done
	go func() {
		stageWg.Wait()
		close(out)
	}()

	return out
}

// SafeStartStage starts a stage, returns an error if no input channels are provided
// func StartStage[I, O any](
// 	name string,
// 	workers int,
// 	fn func(I) O,
// 	c ...<-chan I,
// ) (<-chan O, error) {
// 	// Validate input channels
// 	if len(c) == 0 {
// 		return nil, fmt.Errorf("no input channels provided")
// 	}
// 	var ch <-chan I
// 	if len(c) == 1 {
// 		ch = c[0]
// 	} else {
// 		ch = Merge(c...)
// 	}

// 	// Setup stage workers
// 	out := make(chan O, workers)
// 	var stageWg sync.WaitGroup
// 	stageWg.Add(workers)

// 	// Initialize stage workers
// 	for i := range workers {
// 		go func() {
// 			defer stageWg.Done()
// 			for item := range ch {
// 				fmt.Printf("worker %d starts stage %s\n", i, name)
// 				result := fn(item)
// 				out <- result
// 			}
// 		}()
// 	}

// 	// Close the output channel when stage workers are done
// 	go func() {
// 		stageWg.Wait()
// 		close(out)
// 	}()

// 	return out, nil
// }

func StartStage[I Identifier, O any](
	cfg *Config,
	stageId string,
	workers int,
	fn func(I) (O, error),
	//ec chan error,
	c ...<-chan I,
) (<-chan O, error) {
	// Validate input channels
	if len(c) == 0 {
		//ec <- fmt.Errorf("no input channels provided")
		return nil, fmt.Errorf("no input channels provided")
	}
	var ch <-chan I
	if len(c) == 1 {
		ch = c[0]
	} else {
		ch = Merge(c...)
	}
	// Emit stage started event
	cfg.EmitEvent(NewEventStageStarted(stageId, workers, nil))

	// Setup stage workers
	out := make(chan O, workers)
	var stageWg sync.WaitGroup
	stageWg.Add(workers)

	// Initialize stage workers
	for workerId := range workers {
		go func() {
			cfg.EmitEvent(NewEventWorkerStarted(strconv.Itoa(workerId), stageId, nil))
			defer stageWg.Done()
			for item := range ch {
				cfg.EmitEvent(NewEventItemProcessStarted(item.GetID(), strconv.Itoa(workerId), stageId, nil))
				fmt.Printf("worker %d starts stage %s\n", workerId, stageId)
				result, err := fn(item)
				if err != nil {
					cfg.EmitEvent(NewEventItemProcessFailed(item.GetID(), strconv.Itoa(workerId), stageId, err, nil))
					//ec <- fmt.Errorf("%s stage processing error | %w", stageId, err)
					continue
				}
				cfg.EmitEvent(NewEventItemProcessed(item.GetID(), strconv.Itoa(workerId), stageId, nil))
				out <- result
			}
			cfg.EmitEvent(NewEventWorkerCompleted(strconv.Itoa(workerId), stageId, nil))
		}()
	}

	// Close the output channel when stage workers are done
	go func() {
		stageWg.Wait()
		close(out)
		cfg.EmitEvent(NewEventStageCompleted(stageId, nil))
	}()

	return out, nil
}
