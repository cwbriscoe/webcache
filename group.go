package webcache

import (
	"context"
	"errors"
	"sync"
)

type getter interface {
	get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

type group struct {
	mu     sync.Mutex
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

// do ensures fn() is called only once
func (g *group) do(ctx context.Context, key string) ([]byte, bool, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*call)
	}
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, true, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = g.getter.get(ctx, key)
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.val, false, c.err
}
