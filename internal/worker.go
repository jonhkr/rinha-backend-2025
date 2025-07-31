package internal

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
	Store      Storage
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

const maxTries = 5

func (w *Worker) Process(r *PaymentRequest) (ProcessedPayment, error) {
	defaultProcessor := w.config.Processors[Default]
	for i := 0; i < maxTries; i++ {
		pp, err := defaultProcessor.Process(r)
		if err == nil {
			return pp, nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return w.config.Processors[Fallback].Process(r)
}

func (w *Worker) Run() {
	log.Println("Starting worker")
	w.wg.Add(1)
	defer w.wg.Done()

	for task := range w.config.TaskQueue {
		var request = w.requestPool.Get().(*PaymentRequest)
		err := json.Unmarshal(task.Bytes, request)
		if err != nil {
			log.Println("failed to decode request:", err)
			continue
		}

		pp, err := w.Process(request)
		if err == nil {
			if err = w.config.Store.Save(pp); err != nil {
				log.Println("failed to save request:", err)
				w.config.TaskQueue <- task
			} else {
				w.config.TaskPool.Put(task)
			}
		} else {
			w.config.TaskQueue <- task
		}

		w.requestPool.Put(request)
	}
}
