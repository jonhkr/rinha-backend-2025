package internal

import (
	"github.com/google/btree"
	"time"
)

const (
	Default ProcessorId = iota
	Fallback
)

type Task struct {
	Bytes []byte
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

type PaymentProcessors map[ProcessorId]*PaymentProcessor

type HealthStatus struct {
	Failing         bool        `json:"failing"`
	MinResponseTime int         `json:"minResponseTime"`
	ProcessorId     ProcessorId `json:"processorId"`
}

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

func (pp ProcessedPayment) Less(item btree.Item) bool {
	a := pp
	b := item.(ProcessedPayment)
	if a.CreatedAt.Equal(b.CreatedAt) {
		return a.CorrelationId < b.CorrelationId
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

type PaymentSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type Summary map[ProcessorName]PaymentSummary

func ParseTimeOrDefault(s string, t time.Time) (time.Time, error) {
	if s == "" {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
