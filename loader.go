package dataloader

import (
	"sync"
	"time"
)

type Fetch[T any] func(keys []string) ([]*T, []error)

// LoaderConfig captures the config to create a new Loader
type LoaderConfig struct {
	// Wait is how long wait before sending a batch
	Wait time.Duration

	// Cache Time
	CacheTime time.Duration

	// MaxBatch will limit the maximum number of keys to send in one batch, 0 = not limit
	MaxBatch int
}

func NewConfig() LoaderConfig{
    return LoaderConfig{
        Wait:time.Millisecond,
        CacheTime:time.Second,
        MaxBatch:100,
    }
}


// NewLoader creates a new Loader given a fetch, wait, and maxBatch
func NewLoader[T any](config LoaderConfig,f Fetch[T]) *Loader[T] {
	return &Loader[T]{
		fetch:     f,
		wait:      config.Wait,
		maxBatch:  config.MaxBatch,
		cachetime: config.CacheTime,
		cache:     &sync.Map{},
	}
}

type LoaderCacheItem[T any] struct {
	last time.Time
	v    *T
}

// Loader batches and caches requests
type Loader[T any] struct {
	// this method provides the data for the loader
	fetch Fetch[T]

	// how long to done before sending a batch
	wait time.Duration

	// this will limit the maximum number of keys to send in one batch, 0 = no limit
	maxBatch int

	// INTERNAL

	// lazily created cache
	//cache map[string]*LoaderCacheItem
	cache *sync.Map
	// cache timeout
	cachetime time.Duration

	// the current batch. keys will continue to be collected until timeout is hit,
	// then everything will be sent to the fetch method and out to the listeners
	batch *LoaderBatch[T]

	// mutex to prevent races
	mu sync.Mutex
}

type LoaderBatch[T any] struct {
	keys    []string
	data    []*T
	error   []error
	closing bool
	done    chan struct{}
}

// Load a  by key, batching and caching will be applied automatically
func (l *Loader[T]) Load(key string) (*T, error) {
	return l.LoadThunk(key)()
}

// LoadThunk returns a function that when called will block waiting for a .
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *Loader[T]) LoadThunk(key string) func() (*T, error) {
	if it, ok := l.cache.Load(key); ok {
		return func() (*T, error) {
			iv := it.(*LoaderCacheItem[T])
			iv.last = time.Now()
			return iv.v, nil
		}
	}

	if l.batch == nil {
		l.batch = &LoaderBatch[T]{done: make(chan struct{})}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)

	return func() (*T, error) {
		<-batch.done

		var data *T
		if pos < len(batch.data) {
			data = batch.data[pos]
		}

		var err error
		// its convenient to be able to return a single error for everything
		if len(batch.error) == 1 {
			err = batch.error[0]
		} else if batch.error != nil {
			err = batch.error[pos]
		}

		if err == nil {
			l.unsafeSet(key, data)
		}

		return data, err
	}
}

// LoadAll fetches many keys at once. It will be broken into appropriate sized
// sub batches depending on how the loader is configured
func (l *Loader[T]) LoadAll(keys []string) ([]*T, []error) {
	results := make([]func() (*T, error), len(keys))

	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}

	s := make([]*T, len(keys))
	errors := make([]error, len(keys))
	for i, thunk := range results {
		s[i], errors[i] = thunk()
	}
	return s, errors
}

// LoadAllThunk returns a function that when called will block waiting for a s.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *Loader[T]) LoadAllThunk(keys []string) func() ([]*T, []error) {
	results := make([]func() (*T, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]*T, []error) {
		s := make([]*T, len(keys))
		errors := make([]error, len(keys))
		for i, thunk := range results {
			s[i], errors[i] = thunk()
		}
		return s, errors
	}
}

// Prime the cache with the provided key and value. If the key already exists, no change is made
// and false is returned.
// (To forcefully prime the cache, clear the key first with loader.clear(key).prime(key, value).)
func (l *Loader[T]) Prime(key string, value *T) bool {
	var found bool
	if _, found = l.cache.Load(key); !found {
		// make a copy when writing to the cache, its easy to pass a pointer in from a loop var
		// and end up with the whole cache pointing to the same value.
		cpy := *value
		l.unsafeSet(key, &cpy)
	}
	return !found
}

// Clear the value at key from the cache, if it exists
func (l *Loader[T]) Clear(key string) {
	l.cache.Delete(key)
}

func (l *Loader[T]) unsafeSet(key string, value *T) {
	l.cache.Store(key, &LoaderCacheItem[T]{
		last: time.Now(),
		v:    value,
	})
}

// CacheRotation Rotating cache time
func (l *Loader[T]) CacheRotation(t time.Time) {
	l.cache.Range(func(k, v interface{}) bool {
		iv := v.(*LoaderCacheItem[T])
		if t.Sub(iv.last) > l.cachetime {
			l.cache.Delete(k)
		}
		iv.last = t
		return true
	})
}

// keyIndex will return the location of the key in the batch, if its not found
// it will add the key to the batch
func (b *LoaderBatch[T]) keyIndex(l *Loader[T], key string) int {
	for i, existingKey := range b.keys {
		if key == existingKey {
			return i
		}
	}

	pos := len(b.keys)
	b.keys = append(b.keys, key)
	if pos == 0 {
		go b.startTimer(l)
	}

	if l.maxBatch != 0 && pos >= l.maxBatch-1 {
		if !b.closing {
			b.closing = true
			l.batch = nil
			go b.end(l)
		}
	}

	return pos
}

func (b *LoaderBatch[T]) startTimer(l *Loader[T]) {
	time.Sleep(l.wait)

	// we must have hit a batch limit and are already finalizing this batch
	if b.closing {
		return
	}

	l.batch = nil

	b.end(l)
}

func (b *LoaderBatch[T]) end(l *Loader[T]) {
	b.data, b.error = l.fetch(b.keys)
	close(b.done)
}
