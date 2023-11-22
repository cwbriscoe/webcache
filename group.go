// Copyright 2020 - 2023 Christopher Briscoe.  All rights reserved.

package webcache

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// Getter defines an interface for retrieving emptty cache contents
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// call represents a concurrent function call
type call struct {
	sync.WaitGroup
	err error  // err holds any error encountered during the function call
	val []byte // val holds the result of the function call
}

// group represents a single group of cached values
type group struct {
	sync.Mutex
	getter Getter           // getter is used to retrieve values for the cache
	calls  map[string]*call // calls holds ongoing or completed function calls
	name   string           // name is the name of the group
	maxAge time.Duration    // maxAge is the maximum age of a cached value
}

func newGroup(name string, maxAge time.Duration, getter Getter) (*group, error) {
	if getter == nil {
		return nil, errors.New("getter must not be nil")
	}
	if name == "" {
		return nil, errors.New("name must not be empty")
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

	call := &call{}
	call.Add(1)

	g.calls[key] = call
	g.Unlock()

	// setup recovery in case the getter function panics.
	defer func() {
		if r := recover(); r != nil {
			stackTrace := debug.Stack()
			call.val = []byte("")
			call.err = fmt.Errorf(
				"panic(recovered): (group:'%s',key:'%s') error:\n%v\n%s", g.name, key, r, stackTrace,
			)
			err = call.err
		}
	}()

	call.val, call.err = g.getter.Get(ctx, key)

	return call.val, false, call.err
}

// finish removes a call from the calls map once it's done.
// This prevents the map from growing indefinitely.
func (g *group) finish(key string) {
	g.Lock()
	defer g.Unlock()
	call := g.calls[key]
	call.Done()
	delete(g.calls, key)
}
