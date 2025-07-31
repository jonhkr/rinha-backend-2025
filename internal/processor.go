package internal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	CorrelationIdAlreadyExists = "CorrelationId already exists"
)

type PaymentProcessor struct {
	Id       ProcessorId
	Endpoint string
}

type paymentRequest struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(paymentRequest)
	},
}

func (p *PaymentProcessor) Process(r *PaymentRequest) (ProcessedPayment, error) {
	requestedAt := time.Now().UTC()

	var pp ProcessedPayment
	pp.CorrelationId = r.CorrelationId
	pp.Amount = r.Amount
	pp.CreatedAt = requestedAt
	pp.Processor = p.Id

	req := requestPool.Get().(*paymentRequest)
	req.CorrelationId = r.CorrelationId
	req.Amount = r.Amount
	req.RequestedAt = requestedAt.Format(time.RFC3339Nano)

	b, err := json.Marshal(req)
	if err != nil {
		return pp, err
	}
	requestPool.Put(req)

	resp, err := http.Post(p.Endpoint+"/payments", "application/json", bytes.NewReader(b))
	if err != nil {
		return pp, ErrProcessorFailed
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode == http.StatusUnprocessableEntity {
		body, _ := io.ReadAll(resp.Body)
		if strings.Contains(string(body), CorrelationIdAlreadyExists) {
			return pp, nil
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return pp, ErrProcessorFailed
	}

	return pp, nil
}

func (p *PaymentProcessor) HealthStatus() (HealthStatus, error) {
	var hs HealthStatus
	resp, err := http.Get(p.Endpoint + "/payments/service-health")
	if err != nil {
		return hs, errors.New(fmt.Sprintf("error fetching service health: %s, Id: %d", err, p.Id))
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return hs, errors.New(fmt.Sprintf("received non 200 response from service Id: %d", p.Id))
	}

	if err := json.NewDecoder(resp.Body).Decode(&hs); err != nil {
		return hs, errors.New(fmt.Sprintf("error decoding service health response: %s", err))
	}
	hs.ProcessorId = p.Id
	return hs, nil
}
