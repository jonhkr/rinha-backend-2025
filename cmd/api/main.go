package main

import (
	"encoding/json"
	"github.com/jonhkr/rb2025/internal"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
)

var taskPool = sync.Pool{
	New: func() interface{} {
		buff := make([]byte, 256)
		return &internal.Task{Bytes: buff}
	},
}

func main() {
	socketPath := os.Getenv("SOCKET_PATH")

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

	nWorkers, err := strconv.ParseInt(os.Getenv("WORKERS"), 10, 64)
	if err != nil {
		nWorkers = 5
	}

	internal.PreAllocate[internal.Task](&taskPool, 10_000)

	taskQueue := make(chan *internal.Task, 10_000)

	processors := map[internal.ProcessorId]*internal.PaymentProcessor{
		internal.Default: {
			Id:       internal.Default,
			Endpoint: os.Getenv("PROCESSOR_DEFAULT_URL"),
		},
		internal.Fallback: {
			Id:       internal.Fallback,
			Endpoint: os.Getenv("PROCESSOR_FALLBACK_URL"),
		},
	}

	store := internal.NewSocketStorage("/sockets/db.sock")

	var wg sync.WaitGroup

	workerConf := internal.WorkerConfig{
		TaskQueue:  taskQueue,
		TaskPool:   &taskPool,
		Store:      store,
		Processors: processors,
	}

	for i := 0; i < int(nWorkers); i++ {
		go internal.NewWorker(workerConf, &wg).Run()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) {
		task := taskPool.Get().(*internal.Task)
		_, _ = r.Body.Read(task.Bytes)
		taskQueue <- task
	})

	mux.HandleFunc("GET /payments-summary", func(w http.ResponseWriter, r *http.Request) {
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")

		s, err := store.GetSummary(from, to)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/purge-payments", func(w http.ResponseWriter, r *http.Request) {
		err := store.CleanUp()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	log.Println("Listening on ", socketPath)
	log.Fatal(http.Serve(l, mux))
}
