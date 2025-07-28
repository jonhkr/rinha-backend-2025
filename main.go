package main

import (
	"context"
	"errors"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	jsoniter "github.com/json-iterator/go"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var json = jsoniter.ConfigFastest

func parseTimeOrDefault(s string, t time.Time) (time.Time, error) {
	if s == "" {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

var taskPool = sync.Pool{
	New: func() interface{} {
		buff := make([]byte, 256)
		return &Task{buff}
	},
}

func main() {
	nWorkers, err := strconv.ParseInt(os.Getenv("WORKERS"), 10, 64)
	if err != nil {
		nWorkers = 5
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

	f := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	f.Post("/payments", func(c *fiber.Ctx) error {
		task := taskPool.Get().(*Task)
		copy(task.bytes, c.BodyRaw())
		taskQueue <- task
		return c.SendStatus(fiber.StatusOK)
	})

	f.Get("/payments-summary", func(c *fiber.Ctx) error {
		fromStr := c.Query("from", "")
		toStr := c.Query("to", "")
		from, err := parseTimeOrDefault(fromStr, time.Now().UTC().Add(-24*time.Hour))
		if err != nil {
			_, err := c.Writef("failed to parse time %s: %s", fromStr, err)
			if err != nil {
				return err
			}
			return c.SendStatus(fiber.StatusBadRequest)
		}

		to, err := parseTimeOrDefault(toStr, time.Now().UTC().Add(24*time.Hour))
		if err != nil {
			_, err := c.Writef("failed to parse time %s: %s", fromStr, err)
			if err != nil {
				return err
			}
			return c.SendStatus(fiber.StatusBadRequest)
		}

		s, err := store.GetSummary(from, to)
		if err != nil {
			_, err := c.Writef("failed to get summary: %s", err)
			if err != nil {
				return err
			}
			return c.SendStatus(fiber.StatusInternalServerError)
		}
		return c.JSON(s)
	})

	f.Post("/purge-payments", func(c *fiber.Ctx) error {
		err := store.CleanUp()
		if err != nil {
			return errors.Join(errors.New("failed to clean up database"), err)
		}
		return c.SendStatus(fiber.StatusOK)
	})

	log.Println("Starting server")
	log.Fatal(f.Listen(":8080"))
}
