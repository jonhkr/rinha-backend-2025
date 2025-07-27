package main

import "time"

type queue struct {
	channel chan PaymentRequest
}

func newQueue() *queue {
	return &queue{
		channel: make(chan PaymentRequest, 100_000),
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

type Queue[T any] struct {
	channel chan *T
}

func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{
		channel: make(chan *T, capacity),
	}
}

func (q *Queue[T]) Add(t *T) {
	q.channel <- t
}

func (q *Queue[T]) Poll(timeout time.Duration) (*T, error) {
	select {
	case t := <-q.channel:
		return t, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	}
}
