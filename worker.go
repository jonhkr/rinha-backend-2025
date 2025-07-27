package main

import (
	jsoniter "github.com/json-iterator/go"
	"log"
	"sync"
	"time"
)

var json = jsoniter.ConfigFastest

type WorkerConfig struct {
	TaskQueue  chan *Task
	TaskPool   *sync.Pool
	Processors PaymentProcessors
	Store      storage
}

type Worker struct {
	config      WorkerConfig
	requestPool *sync.Pool
	wg          *sync.WaitGroup
}

func NewWorker(config WorkerConfig, wg *sync.WaitGroup) *Worker {
	requestPool := &sync.Pool{
		New: func() interface{} {
			return &PaymentRequest{}
		},
	}
	PreAllocate[PaymentRequest](requestPool, 2000)
	return &Worker{
		config:      config,
		requestPool: requestPool,
		wg:          wg,
	}
}

const maxTries = 10

func (w *Worker) Run() {
	log.Println("Starting worker")
	w.wg.Add(1)
	defer w.wg.Done()

	for task := range w.config.TaskQueue {
		var processor = w.config.Processors[Default]

		var request = w.requestPool.Get().(*PaymentRequest)
		err := json.Unmarshal(task.bytes, request)
		if err != nil {
			log.Println("failed to decode request:", err)
			continue
		}

		i := 0
		for {
			if i >= maxTries {
				w.config.TaskQueue <- task
				break
			}
			pp, err := processor.Process(request)
			if err != nil {
				i++
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if err = w.config.Store.Save(pp); err != nil {
				i++
				log.Println("failed to save request:", err)
				continue
			}

			break
		}

		w.requestPool.Put(request)
		w.config.TaskPool.Put(task)
	}
}
