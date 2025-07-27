package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type PaymentHandler struct {
	taskQueue chan *Task
	taskPool  *sync.Pool
}

func NewPaymentHandler(tq chan *Task, taskPool *sync.Pool) *PaymentHandler {
	return &PaymentHandler{
		taskQueue: tq,
		taskPool:  taskPool,
	}
}

func (h *PaymentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	task := h.taskPool.Get().(*Task)
	_, err := r.Body.Read(task.bytes)
	if err != nil && err != io.EOF {
		http.Error(w, fmt.Sprintf("failed to read body: %s", err), http.StatusBadRequest)
		return
	}

	h.taskQueue <- task
	return
}

type summaryHandler struct {
	store storage
}

func parseTimeOrDefault(s string, t time.Time) (time.Time, error) {
	if s == "" {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func (h *summaryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	from, err := parseTimeOrDefault(fromStr, time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse time %s: %s", fromStr, err), http.StatusBadRequest)
		return
	}

	to, err := parseTimeOrDefault(toStr, time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse time %s: %s", toStr, err), http.StatusBadRequest)
		return
	}

	s, err := h.store.GetSummary(from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	nWorkers, err := strconv.ParseInt(os.Getenv("WORKERS"), 10, 64)
	if err != nil {
		nWorkers = 5
	}

	taskPool := sync.Pool{
		New: func() interface{} {
			buff := make([]byte, 256)
			return &Task{buff}
		},
	}

	PreAllocate[Task](&taskPool, 10_000)

	taskQueue := make(chan *Task, 10_000)

	processors := map[ProcessorId]*paymentProcessor{
		Default: {
			id:       Default,
			endpoint: os.Getenv("PROCESSOR_DEFAULT_URL"),
		},
		Fallback: {
			id:       Fallback,
			endpoint: os.Getenv("PROCESSOR_FALLBACK_URL"),
		},
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	store := &pgStorage{
		db: pool,
	}

	var wg sync.WaitGroup

	workerConf := WorkerConfig{
		TaskQueue:  taskQueue,
		TaskPool:   &taskPool,
		Store:      store,
		Processors: processors,
	}

	for i := 0; i < int(nWorkers); i++ {
		go NewWorker(workerConf, &wg).Run()
	}

	router := httprouter.New()
	router.POST("/payments", NewPaymentHandler(taskQueue, &taskPool).ServeHTTP)
	router.GET("/payments-summary", (&summaryHandler{store: store}).ServeHTTP)
	router.POST("/purge-payments", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		err := store.CleanUp()
		if err != nil {
			log.Println("failed to purge payments: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	log.Println("Starting server")
	log.Fatal(http.ListenAndServe(":8080", router))
}
