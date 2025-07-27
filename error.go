package main

import "errors"

var (
	ErrTimeout         = errors.New("timed out")
	ErrProcessorFailed = errors.New("processors failed")
)
