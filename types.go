package main

import "time"

const (
	Default ProcessorId = iota
	Fallback
)

type Task struct {
	bytes []byte
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

type PaymentProcessors map[ProcessorId]*paymentProcessor

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

type PaymentSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type Summary map[ProcessorName]PaymentSummary
