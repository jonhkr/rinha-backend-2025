package main

import "time"

type storage interface {
	Save(ProcessedPayment) error
	GetSummary(from, to time.Time) (Summary, error)
	CleanUp() error
}
