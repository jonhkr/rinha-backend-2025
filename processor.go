package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type paymentProcessor struct {
	id       ProcessorId
	endpoint string
}

func (p *paymentProcessor) Process(r *PaymentRequest) (ProcessedPayment, error) {
	requestedAt := time.Now().UTC()

	var pp ProcessedPayment
	pp.CorrelationId = r.CorrelationId
	pp.Amount = r.Amount
	pp.CreatedAt = requestedAt
	pp.Processor = p.id

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
