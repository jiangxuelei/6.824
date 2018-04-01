package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	var mu = new(sync.Mutex)
	srvs := make([]string, 0)

	go func() {
		for srv := range registerChan {
			mu.Lock()
			srvs = append(srvs, srv)
			mu.Unlock()
		}
	}()

	for k,v := range mapFiles {

		for ;; {
			if len(srvs) <= 0 {
				time.Sleep(1 * time.Millisecond)
			} else {
				break
			}
		}
		mu.Lock()
		addr := srvs[0]; srvs = srvs[1:]
		mu.Unlock()
		args := DoTaskArgs{JobName: jobName, File: v, Phase: phase, TaskNumber: k, NumOtherPhase: n_other}
		go func(addr string, args DoTaskArgs) {
			wg.Add(1)
			call(addr, "Worker.DoTask", args, nil)
			mu.Lock()
			srvs = append(srvs, addr)
			mu.Unlock()
			wg.Done()
		}(addr, args)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
