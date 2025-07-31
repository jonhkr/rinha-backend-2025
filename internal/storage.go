package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Storage interface {
	Save(ProcessedPayment) error
	GetSummary(from, to string) (Summary, error)
	CleanUp() error
}

type socketStorage struct {
	client     *http.Client
	socketPath string
	baseURL    string
}

func NewSocketStorage(socketPath string) Storage {
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", socketPath)
		},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}
	return &socketStorage{
		client:     client,
		socketPath: socketPath,
		baseURL:    "http://unix", // Dummy host required for request construction
	}
}

const (
	paymentsUrl = "http://unix/payments"
	summaryUrl  = "http://unix/summary"
	cleanUpUrl  = "http://unix/clean-up"
)

// Save sends a payment via HTTP POST to /payment
func (s *socketStorage) Save(pp ProcessedPayment) error {
	body, err := json.Marshal(pp)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, paymentsUrl, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("save failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("save error: %s", data)
	}
	return nil
}

func (s *socketStorage) GetSummary(from, to string) (Summary, error) {
	query := url.Values{}
	query.Set("from", from)
	query.Set("to", to)
	builder := strings.Builder{}
	builder.WriteString(summaryUrl)
	builder.WriteString("?")
	builder.WriteString(query.Encode())

	req, err := http.NewRequest(http.MethodGet, builder.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("summary failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("summary error: %s", data)
	}

	var summary Summary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nil, fmt.Errorf("invalid summary response: %w", err)
	}
	return summary, nil
}

func (s *socketStorage) CleanUp() error {
	_, err := http.NewRequest(http.MethodDelete, cleanUpUrl, nil)
	return err
}
