package main

import (
	"github.com/google/btree"
	"github.com/jonhkr/rb2025/internal"
	jsoniter "github.com/json-iterator/go"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

var json = jsoniter.ConfigFastest
var store = NewStorage()

func main() {
	socketPath := "/sockets/db.sock"

	if _, err := os.Stat(socketPath); err == nil {
		_ = os.Remove(socketPath)
	}

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatal("Failed to listen on socket: ", err)
	}
	defer os.Remove(socketPath)
	defer l.Close()

	err = os.Chmod(socketPath, 0766)
	if err != nil {
		log.Fatal("Failed to chmod socket: ", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) {
		var pp internal.ProcessedPayment
		if err := json.NewDecoder(r.Body).Decode(&pp); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// ignored as no error is ever returned
		_ = store.Put(pp)
	})
	mux.HandleFunc("GET /summary", func(w http.ResponseWriter, r *http.Request) {
		fromStr := r.URL.Query().Get("from")
		toStr := r.URL.Query().Get("to")
		from, err := internal.ParseTimeOrDefault(fromStr, time.Now().UTC().Add(-24*time.Hour))
		if err != nil {
			log.Println("Failed to parse from time: ", fromStr, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		to, err := internal.ParseTimeOrDefault(toStr, time.Now().UTC().Add(-24*time.Hour))
		if err != nil {
			log.Println("Failed to parse to time: ", toStr, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		summary := store.Summary(from, to)
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(summary)
		if err != nil {
			log.Println("Failed to encode summary: ", err)
		}
	})
	mux.HandleFunc("POST /clean-up", func(w http.ResponseWriter, r *http.Request) {
		_ = store.CleanUp()
	})

	log.Println("Listening on", socketPath)
	log.Fatal(http.Serve(l, mux))
}

type Storage struct {
	tree  *btree.BTree
	keys  map[string]struct{}
	mutex sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		tree: btree.New(2),
		keys: make(map[string]struct{}),
	}
}

func (s *Storage) Put(pp internal.ProcessedPayment) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.keys[pp.CorrelationId]; exists {
		return nil
	}

	s.keys[pp.CorrelationId] = struct{}{}
	s.tree.ReplaceOrInsert(pp)
	return nil
}

func (s *Storage) Summary(from, to time.Time) internal.Summary {
	summary := internal.Summary{
		internal.Default.Name():  {},
		internal.Fallback.Name(): {},
	}

	s.tree.AscendGreaterOrEqual(internal.ProcessedPayment{CreatedAt: from}, func(item btree.Item) bool {
		pp := item.(internal.ProcessedPayment)

		if pp.CreatedAt.After(to) {
			return false
		}

		ppName := pp.Processor.Name()
		entry := summary[ppName]
		entry.TotalRequests++
		entry.TotalAmount += pp.Amount
		summary[ppName] = entry
		return true
	})

	return summary
}

func (s *Storage) CleanUp() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.tree.Clear(false)
	s.keys = make(map[string]struct{})
	return nil
}
