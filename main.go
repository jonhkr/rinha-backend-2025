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

type Node struct {
	endpoint string
}

type healthChecker struct {
	processors PaymentProcessors
	statuses   map[ProcessorId]HealthStatus
	nodes      []Node
	s          chan struct{}
	wg         *sync.WaitGroup
}

func (h *healthChecker) Stop() {
	h.s <- struct{}{}
}

func (h *healthChecker) GetStatus(id ProcessorId) HealthStatus {
	hs, ok := h.statuses[id]
	if !ok {
		return HealthStatus{}
	}
	return hs
}

func (h *healthChecker) Store(hs HealthStatus) {
	h.statuses[hs.ProcessorId] = hs
}

func (h *healthChecker) StoreAndPropagate(hs HealthStatus) {
	h.Store(hs)
	for _, node := range h.nodes {
		if jsonBytes, err := json.Marshal(hs); err == nil {
			_, _ = http.Post(node.endpoint+"/_internal/service-health",
				"application/json", bytes.NewReader(jsonBytes))
		}
	}
}

func (h *healthChecker) Run() {
	log.Println("Starting health checker")
	h.wg.Add(1)
	defer h.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.s:
			log.Println("Stopping health checker")
			return
		case <-ticker.C:
			for _, processor := range h.processors {
				hs, err := processor.HealthStatus()
				if err != nil {
					log.Println(err)
					continue
				}
				h.StoreAndPropagate(hs)
			}
		}
	}
}

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
		log.Println("Error processing request:", err)
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
		body, _ := io.ReadAll(resp.Body)
		log.Println("Error processing request:", resp.StatusCode, string(body))
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
	log.Println("Checking health status of:", p.id)
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

const (
	Default ProcessorId = iota
	Fallback
)

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
		channel: make(chan PaymentRequest, 10000),
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
	hc        *healthChecker
}

type worker struct {
	queue     *queue
	processor PaymentProcessors
	store     storage
	hc        *healthChecker
	wg        *sync.WaitGroup
	s         chan struct{}
}

func newWorker(c workerConfig, wg *sync.WaitGroup) *worker {
	return &worker{
		queue:     c.queue,
		processor: c.processor,
		store:     c.store,
		hc:        c.hc,
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
		if r, err := w.queue.poll(1 * time.Second); err == nil {
			var processor = w.processor[Default]
			defaultStatus := w.hc.GetStatus(Default)
			fallbackStatus := w.hc.GetStatus(Fallback)

			if defaultStatus.Failing || defaultStatus.MinResponseTime > fallbackStatus.MinResponseTime {
				processor = w.processor[Fallback]
			}

			p, err := processor.Process(r)
			if err != nil && errors.Is(err, ErrProcessorFailed) {
				w.hc.StoreAndPropagate(HealthStatus{
					Failing:     true,
					ProcessorId: p.Processor,
				})
			}
			if err != nil {
				log.Println("Error processing payment:", err)
				w.queue.add(r)
				continue
			}
			err = w.store.Save(p)
			if err != nil {
				log.Println("failed to save payment:", err)
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

	nodeEndpoints := strings.Split(os.Getenv("NODE_LIST"), ",")
	nodes := make([]Node, len(nodeEndpoints))
	for i, endpoint := range nodeEndpoints {
		nodes[i] = Node{
			endpoint: endpoint,
		}
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	store := &pgStorage{
		db: pool,
	}

	var wg sync.WaitGroup
	hc := &healthChecker{
		processors: processors,
		nodes:      nodes,
		statuses: map[ProcessorId]HealthStatus{
			Default:  {},
			Fallback: {MinResponseTime: 100},
		},
		wg: &wg,
		s:  make(chan struct{}, 1),
	}

	if os.Getenv("MASTER_NODE") == "true" {
		err = store.Init()
		if err != nil {
			log.Fatal(err)
		}
		go hc.Run()
	}

	workerConf := workerConfig{
		queue:     requestQueue,
		processor: processors,
		store:     store,
		hc:        hc,
	}

	workers := []*worker{
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
	})
	router.HandleFunc("/_internal/service-health", func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var hs HealthStatus
		err = json.Unmarshal(bodyBytes, &hs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Println(hs)
		hc.Store(hs)
	})

	go func() {
		var captureSignal = make(chan os.Signal, 1)
		signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
		signalHandler(<-captureSignal, workers, hc, &wg)
	}()

	log.Println("Starting server")
	log.Println(http.ListenAndServe(":8080", router))
}

func signalHandler(signal os.Signal, workers []*worker, hc *healthChecker, wg *sync.WaitGroup) {
	log.Printf("Caught signal: %+v\n", signal)
	log.Println("Shutting down workers...")

	for _, w := range workers {
		w.DrainAndStop()
	}

	hc.Stop()
	wg.Wait()

	log.Println("Finished server cleanup, exiting...")
	os.Exit(0)
}
