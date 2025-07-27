package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrTimeout         = errors.New("timed out")
	ErrProcessorFailed = errors.New("processor failed")
)

type paymentProcessor struct {
	id       ProcessorId
	endpoint string
}

func (p *paymentProcessor) Process(r PaymentRequest) (ProcessedPayment, error) {
	var pp ProcessedPayment

	requestedAt := time.Now().UTC()
	pp.CorrelationId = r.CorrelationId
	pp.Amount = r.Amount
	pp.Processor = p.id
	pp.CreatedAt = requestedAt

	req := map[string]interface{}{
		"correlationId": r.CorrelationId,
		"amount":        r.Amount,
		"requestedAt":   requestedAt.Format(time.RFC3339Nano),
	}
	b, err := json.Marshal(req)
	if err != nil {
		return pp, err
	}

	resp, err := http.Post(p.endpoint+"/payments", "application/json", bytes.NewReader(b))
	if err != nil {
		return pp, ErrProcessorFailed
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode == http.StatusUnprocessableEntity {
		body, _ := io.ReadAll(resp.Body)
		if strings.Contains(string(body), "CorrelationId already exists") {
			return pp, nil
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return pp, ErrProcessorFailed
	}

	return pp, nil
}

type HealthStatus struct {
	Failing         bool        `json:"failing"`
	MinResponseTime int         `json:"minResponseTime"`
	ProcessorId     ProcessorId `json:"processorId"`
}

func (p *paymentProcessor) HealthStatus() (HealthStatus, error) {
	var hs HealthStatus
	resp, err := http.Get(p.endpoint + "/payments/service-health")
	if err != nil {
		return hs, errors.New(fmt.Sprintf("error fetching service health: %s, id: %d", err, p.id))
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return hs, errors.New(fmt.Sprintf("received non 200 response from service id: %d", p.id))
	}

	if err := json.NewDecoder(resp.Body).Decode(&hs); err != nil {
		return hs, errors.New(fmt.Sprintf("error decoding service health response: %s", err))
	}
	hs.ProcessorId = p.id
	return hs, nil
}

type ProcessorId int
type ProcessorName string

const (
	Default ProcessorId = iota
	Fallback
)

func (p ProcessorId) Name() ProcessorName {
	switch p {
	case Default:
		return "default"
	case Fallback:
		return "fallback"
	default:
		return ""
	}
}

type PaymentProcessors map[ProcessorId]*paymentProcessor

type PaymentRequest struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type ProcessedPayment struct {
	CorrelationId string
	Amount        float64
	Processor     ProcessorId
	CreatedAt     time.Time
}

type PaymentSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type Summary map[ProcessorName]PaymentSummary

type storage interface {
	Save(ProcessedPayment) error
	GetSummary(from, to time.Time) (Summary, error)
	CleanUp() error
}

type pgStorage struct {
	db *pgxpool.Pool
}

const (
	schemaSql = `
		CREATE TABLE IF NOT EXISTS payments (
			correlation_id TEXT NOT NULL,
			amount DECIMAL(12, 2) NOT NULL,
			processor_id SMALLINT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			PRIMARY KEY(correlation_id)
		)`
	insertSql = `INSERT INTO payments(correlation_id, amount, processor_id, created_at) 
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`
	summarySql = `
		SELECT processor_id, COUNT(*), SUM(amount)
		FROM payments WHERE created_at BETWEEN $1 AND $2
		GROUP BY processor_id`
	deleteAllSql = `DELETE FROM payments WHERE processor_id in (0,1)`
)

func (s *pgStorage) Save(p ProcessedPayment) error {
	ctx := context.Background()
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	_, err = conn.Conn().Exec(ctx, insertSql, p.CorrelationId, p.Amount, p.Processor, p.CreatedAt)
	if err != nil {
		return err
	}
	return nil
}

func (s *pgStorage) GetSummary(from, to time.Time) (Summary, error) {
	ctx := context.Background()
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	var summary = Summary{
		Default.Name():  {},
		Fallback.Name(): {},
	}
	rows, err := conn.Conn().Query(ctx, summarySql, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid ProcessorId
		var ps PaymentSummary
		err = rows.Scan(&pid, &ps.TotalRequests, &ps.TotalAmount)
		if err != nil {
			return nil, err
		}
		summary[pid.Name()] = ps
	}

	return summary, nil
}

func (s *pgStorage) CleanUp() error {
	ctx := context.Background()
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().Exec(ctx, deleteAllSql)
	if err != nil {
		return err
	}
	return nil
}

func (s *pgStorage) Init() error {
	ctx := context.Background()
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().Exec(ctx, schemaSql)
	if err != nil {
		return err
	}
	return nil
}

type queue struct {
	channel chan PaymentRequest
}

func newQueue() *queue {
	return &queue{
		channel: make(chan PaymentRequest, 100_000),
	}
}

func (q *queue) add(payment PaymentRequest) {
	q.channel <- payment
}

func (q *queue) poll(timeout time.Duration) (PaymentRequest, error) {
	select {
	case payment := <-q.channel:
		return payment, nil
	case <-time.After(timeout):
		return PaymentRequest{}, ErrTimeout
	}
}

type workerConfig struct {
	queue     *queue
	processor PaymentProcessors
	store     storage
}

type worker struct {
	queue     *queue
	processor PaymentProcessors
	store     storage
	wg        *sync.WaitGroup
	s         chan struct{}
}

func newWorker(c workerConfig, wg *sync.WaitGroup) *worker {
	return &worker{
		queue:     c.queue,
		processor: c.processor,
		store:     c.store,
		wg:        wg,
		s:         make(chan struct{}, 1),
	}
}

func (w *worker) DrainAndStop() {
	w.s <- struct{}{}
}

func (w *worker) shouldStop() bool {
	select {
	case <-w.s:
		log.Println("Stop signal received")
		return true
	default:
		return false
	}
}

func (w *worker) Run() {
	log.Println("Starting worker")
	w.wg.Add(1)
	defer w.wg.Done()
	for {
		if r, err := w.queue.poll(100 * time.Millisecond); err == nil {
			var processor = w.processor[Default]

			for i := 0; i < 10; i++ {
				p, err := processor.Process(r)
				if err == nil {
					err = w.store.Save(p)
					if err != nil {
						log.Println("failed to save payment:", err)
					}
					break
				}
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}

			if err != nil {
				w.queue.add(r)
				continue
			}

		} else {
			if w.shouldStop() {
				return
			}
		}
	}
}

type paymentHandler struct {
	queue *queue
}

func (h *paymentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var payment PaymentRequest
	err = json.Unmarshal(bodyBytes, &payment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.queue.add(payment)
	return
}

type summaryHandler struct {
	store storage
}

func (h *summaryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse time %s: %s", fromStr, err), http.StatusBadRequest)
		return
	}

	to, err := time.Parse(time.RFC3339, toStr)
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
	requestQueue := newQueue()
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

	if os.Getenv("MASTER_NODE") == "true" {
		err = store.Init()
		if err != nil {
			log.Fatal(err)
		}
	}

	workerConf := workerConfig{
		queue:     requestQueue,
		processor: processors,
		store:     store,
	}

	workers := []*worker{
		newWorker(workerConf, &wg),
		newWorker(workerConf, &wg),
		newWorker(workerConf, &wg),
		newWorker(workerConf, &wg),
		newWorker(workerConf, &wg),
	}

	for _, w := range workers {
		go w.Run()
	}

	router := http.NewServeMux()
	router.Handle("/payments", &paymentHandler{queue: requestQueue})
	router.Handle("/payments-summary", &summaryHandler{store: store})
	router.HandleFunc("/purge-payments", func(w http.ResponseWriter, r *http.Request) {
		err := store.CleanUp()
		if err != nil {
			log.Println("failed to purge payments: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	go func() {
		var captureSignal = make(chan os.Signal, 1)
		signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
		signalHandler(<-captureSignal, workers, &wg)
	}()

	log.Println("Starting server")
	log.Println(http.ListenAndServe(":8080", router))
}

func signalHandler(signal os.Signal, workers []*worker, wg *sync.WaitGroup) {
	log.Printf("Caught signal: %+v\n", signal)
	log.Println("Shutting down workers...")

	for _, w := range workers {
		w.DrainAndStop()
	}

	wg.Wait()

	log.Println("Finished server cleanup, exiting...")
	os.Exit(0)
}
