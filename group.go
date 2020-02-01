// Copyright 2020 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"errors"
	"sync"
)

type getter interface {
	get(ctx context.Context, key string) ([]byte, error)
}

type call struct {
	sync.WaitGroup
	val []byte
	err error
}

type group struct {
	sync.Mutex
	name   string
	getter getter
	calls  map[string]*call
}

func newGroup(name string, getter getter) (*group, error) {
	if getter == nil {
		return nil, errors.New("getter must not be nil")
	}

	return &group{
		name:   name,
		getter: getter,
	}, nil
}

// do ensures fn() is called only once per group key
func (g *group) do(ctx context.Context, key string) ([]byte, bool, error) {
	g.Lock()

	if g.calls == nil {
		g.calls = make(map[string]*call)
	}

	if c, ok := g.calls[key]; ok {
		g.Unlock()
		c.Wait()
		return c.val, true, c.err
	}

	c := new(call)
	c.Add(1)
	g.calls[key] = c
	g.Unlock()

	c.val, c.err = g.getter.get(ctx, key)
	c.Done()

	g.Lock()
	delete(g.calls, key)
	g.Unlock()

	return c.val, false, c.err
}
