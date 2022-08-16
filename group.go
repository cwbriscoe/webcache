// Copyright 2020 - 2022 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"errors"
	"fmt"
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

// do ensures fn() is called only once per group key (singleflight).
func (g *group) do(ctx context.Context, key string) (_ []byte, _ bool, err error) {
	g.Lock()

	if call, ok := g.calls[key]; ok {
		g.Unlock()
		call.Wait()
		return call.val, true, call.err
	}

	call := new(call)
	call.Add(1)

	g.calls[key] = call
	g.Unlock()

	// setup recovery in case the getter function panics.
	defer func() {
		if i := recover(); i != nil {
			call.val = []byte("")
			call.err = fmt.Errorf(
				"panic(recovered): (group:'%s',key:'%s') error:\n%v", g.name, key, i,
			)
			err = call.err
		}
	}()

	call.val, call.err = g.getter.Get(ctx, key)

	return call.val, false, call.err
}

func (g *group) finish(key string) {
	g.Lock()
	call := g.calls[key]
	call.Done()
	delete(g.calls, key)
	g.Unlock()
}
