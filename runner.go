package main

import (
	"sync"

	"github.com/rs/zerolog/log"
)

type task struct {
	fn  func(id, min, max int64)
	min int64
	max int64
}

func (r *Runner) addTask(fn func(id, min, max int64), min, max int64) {
	log.Debug().Msgf("adding task %v %v %v", fn, min, max)
	r.tasks <- task{fn, min, max}
	log.Debug().Msgf("task added %v %v %v", fn, min, max)
}

type Runner struct {
	workers int
	tasks   chan task
	wg      *sync.WaitGroup
}

func NewRunner(workers int) *Runner {
	tasks := make(chan task, workers)
	return &Runner{
		workers: workers,
		tasks:   tasks,
		wg:      &sync.WaitGroup{},
	}
}

func (r *Runner) Run() {

	for i := 0; i < r.workers; i++ {
		go func(id int) {
			r.wg.Add(1)
			log.Debug().Msgf("worker ID %d is now active", id)
			for t := range r.tasks {
				log.Debug().Msgf("worker ID %d running task %v %v", id, t.min, t.max)
				t.fn(int64(i), t.min, t.max)
			}
			log.Debug().Msgf("worker ID %d is exiting", i)
			r.wg.Done()
		}(i)
	}
}
