package main

import "sync"

func PreAllocate[T any](pool *sync.Pool, qty int) {
	var t = make([]*T, qty)
	for i := 0; i < qty; i++ {
		t[i] = pool.Get().(*T)
	}
	for i := 0; i < qty; i++ {
		pool.Put(t[i])
	}
}
