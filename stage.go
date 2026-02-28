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
//

type StageConfig[I Identifier, O Identifier] struct {
	cfg     *Config
	Workers int
	StageId string
	fn      func(I) (O, error)
	c       []<-chan I
}

// NewStageConfig creates a new StageConfig with the given parameters.
func NewStageConfig[I Identifier, O Identifier](
	cfg *Config,
	stageId string,
	workers int,
	fn func(I) (O, error),
	channels ...<-chan I,
) StageConfig[I, O] {
	return StageConfig[I, O]{
		cfg:     cfg,
		StageId: stageId,
		Workers: workers,
		fn:      fn,
		c:       channels,
	}
}

func StartStage[I Identifier, O Identifier](sc StageConfig[I, O]) (<-chan O, error) {
	// Validate input channels
	if len(sc.c) == 0 {
		return nil, fmt.Errorf("no input channels provided")
	}

	// Emit stage started event
	sc.cfg.EmitEvent(NewEventStageStarted(sc.StageId, sc.Workers, nil))

	// Setup stage workers
	out := make(chan O, sc.Workers)
	var stageWg sync.WaitGroup
	stageWg.Add(sc.Workers)

	// Initialize stage workers
	for workerId := 0; workerId < sc.Workers; workerId++ {
		go StartWorker(WorkerConfig[I, O]{
			StageConfig: sc,
			WorkerId:    workerId,
			StageWg:     &stageWg,
			Out:         out,
		})
	}

	// Close the output channel when stage workers are done
	go func() {
		stageWg.Wait()
		close(out)
		sc.cfg.EmitEvent(NewEventStageCompleted(sc.StageId, nil))
	}()

	return out, nil
}

type WorkerConfig[I Identifier, O Identifier] struct {
	StageConfig[I, O]
	WorkerId int
	StageWg  *sync.WaitGroup
	Out      chan O
}

func StartWorker[I Identifier, O Identifier](wc WorkerConfig[I, O]) {
	var ch <-chan I
	if len(wc.c) == 1 {
		ch = wc.c[0]
	} else {
		ch = Merge(wc.c...)
	}
	wc.cfg.EmitEvent(NewEventWorkerStarted(strconv.Itoa(wc.WorkerId), wc.StageId, nil))
	defer wc.StageWg.Done()
	select {
	case <-wc.cfg.Context.Done():
		return
	default:
		for item := range ch {
			StartProcess(ProcessConfig[I, O]{WorkerConfig: wc, Item: item})
		}
		wc.cfg.EmitEvent(NewEventWorkerCompleted(strconv.Itoa(wc.WorkerId), wc.StageId, nil))
	}
}

type ProcessConfig[I Identifier, O Identifier] struct {
	WorkerConfig[I, O]
	Item I
}

func StartProcess[I Identifier, O Identifier](pc ProcessConfig[I, O]) {
	pc.cfg.EmitEvent(NewEventProcessStarted(pc.Item.GetID(), strconv.Itoa(pc.WorkerId), pc.StageId, nil))
	fmt.Printf("worker %d starts stage %s\n", pc.WorkerId, pc.StageId)
	result, err := pc.fn(pc.Item)
	if err != nil {
		pc.cfg.EmitEvent(NewEventProcessFailed(pc.Item.GetID(), strconv.Itoa(pc.WorkerId), pc.StageId, err, nil))
		return
	}
	pc.cfg.EmitEvent(NewEventProcessCompleted(pc.Item.GetID(), strconv.Itoa(pc.WorkerId), pc.StageId, nil))
	pc.Out <- result
}
