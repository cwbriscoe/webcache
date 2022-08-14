// Copyright 2020 - 2022 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"errors"
	"sync"
	"time"
)

type getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type call struct {
	sync.WaitGroup
	err error
	val []byte
}

type group struct {
	sync.Mutex
	getter getter
	calls  map[string]*call
	name   string
	maxAge time.Duration
}

func newGroup(name string, maxAge time.Duration, getter getter) (*group, error) {
	if getter == nil {
		return nil, errors.New("getter must not be nil")
	}

	return &group{
		name:   name,
		maxAge: maxAge,
		getter: getter,
		calls:  make(map[string]*call),
	}, nil
}

// do ensures fn() is called only once per group key (singleflight)
func (g *group) do(ctx context.Context, key string) ([]byte, bool, error) {
	g.Lock()

	if c, ok := g.calls[key]; ok {
		g.Unlock()
		c.Wait()
		return c.val, true, c.err
	}

	c := new(call)
	c.Add(1)
	g.calls[key] = c
	g.Unlock()

	c.val, c.err = g.getter.Get(ctx, key)
	c.Done()

	g.Lock()
	delete(g.calls, key)
	g.Unlock()

	return c.val, false, c.err
}
