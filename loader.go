package dataloader

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Fetch[K comparable, R any] func(keys []K) ([]*R, []error)

type Cache interface {
	SaveExpire(string, time.Duration, []byte)
	GetExpire(string, time.Duration) ([]byte, bool)
	Clear(...string)
}

// LoaderConfig captures the config to create a new Loader
type Config struct {
	// Wait is how long wait before sending a batch
	Wait time.Duration

	// Cache Time
	CacheTime time.Duration

	// MaxBatch will limit the maximum number of keys to send in one batch, 0 = not limit
	MaxBatch int

	// Cache Key prefix
	Prefix string
}

func (c Config) NewWait(w time.Duration) Config {
	d := c
	d.Wait = w
	return d
}
func (c Config) NewCacheTime(t time.Duration) Config {
	d := c
	d.CacheTime = t
	return d
}
func (c Config) NewMaxBatch(m int) Config {
	d := c
	d.MaxBatch = m
	return d
}
func (c Config) NewPrefix(p string) Config {
	d := c
	d.Prefix = p
	return d
}
func (c Config) WithPrefix(p string) Config {
	d := c
	d.Prefix += p
	return d
}

// NewLoader creates a new Loader given a fetch, wait, and maxBatch
func NewLoader[K comparable, R any](config Config, cache Cache, f Fetch[K, R]) *Loader[K, R] {
	return &Loader[K, R]{
		fetch:     f,
		wait:      config.Wait,
		maxBatch:  config.MaxBatch,
		cachetime: config.CacheTime,
		cache:     cache,
		prefix:    config.Prefix,
	}
}

// Loader batches and caches requests
type Loader[K comparable, R any] struct {
	// this method provides the data for the loader
	fetch Fetch[K, R]

	// how long to done before sending a batch
	wait time.Duration

	// this will limit the maximum number of keys to send in one batch, 0 = no limit
	maxBatch int

	prefix string
	// INTERNAL

	cache Cache
	// cache timeout
	cachetime time.Duration

	// the current batch. keys will continue to be collected until timeout is hit,
	// then everything will be sent to the fetch method and out to the listeners
	batch *LoaderBatch[K, R]
}

type LoaderBatch[K comparable, R any] struct {
	keys    []K
	data    []*R
	error   []error
	closing bool
	done    chan struct{}
	one     *sync.Once
}

func (l *Loader[K, R]) Key(key K) string {
	return fmt.Sprintf("%v%v", l.prefix, key)
}

// Load a  by key, batching and caching will be applied automatically
func (l *Loader[K, R]) Load(key K) (*R, error) {
	return l.LoadThunk(key)()
}

func loaderWithBytes[T any](value []byte) *T {
	if string(value) == "" {
		return nil
	}

	o := new(T)
	json.Unmarshal(value, o)
	return o
}

// LoadThunk returns a function that when called will block waiting for a .
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *Loader[K, R]) LoadThunk(key K) func() (*R, error) {
	if it, ok := l.cache.GetExpire(l.Key(key), l.cachetime); ok {
		return func() (*R, error) {
			return loaderWithBytes[R](it), nil
		}
	}

	if l.batch == nil {
		l.batch = &LoaderBatch[K, R]{
			done: make(chan struct{}),
			one:  &sync.Once{},
		}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)

	return func() (*R, error) {
		<-batch.done

		var data *R
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
func (l *Loader[K, R]) LoadAll(keys []K) ([]*R, []error) {
	results := make([]func() (*R, error), len(keys))

	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}

	s := make([]*R, len(keys))
	errors := make([]error, len(keys))
	for i, thunk := range results {
		s[i], errors[i] = thunk()
	}
	return s, errors
}

// LoadAllThunk returns a function that when called will block waiting for a s.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *Loader[K, R]) LoadAllThunk(keys []K) func() ([]*R, []error) {
	results := make([]func() (*R, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]*R, []error) {
		s := make([]*R, len(keys))
		errors := make([]error, len(keys))
		for i, thunk := range results {
			s[i], errors[i] = thunk()
		}
		return s, errors
	}
}

// Clear the value at key from the cache, if it exists
func (l *Loader[K, R]) Clear(keys ...K) {
	nk := []string{}
	for _, v := range keys {
		nk = append(nk, l.Key(v))
	}

	l.cache.Clear(nk...)
}

func (l *Loader[K, R]) unsafeSet(key K, value *R) {
	data := []byte{}
	if value != nil {
		data, _ = json.Marshal(value)
	}
	l.cache.SaveExpire(l.Key(key), l.cachetime, data)
}

// keyIndex will return the location of the key in the batch, if its not found
// it will add the key to the batch
func (b *LoaderBatch[K, R]) keyIndex(l *Loader[K, R], key K) int {
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
			l.batch = nil
			b.closing = true
			go b.end(l)
		}
	}

	return pos
}

func (b *LoaderBatch[K, R]) startTimer(l *Loader[K, R]) {
	time.Sleep(l.wait)

	// we must have hit a batch limit and are already finalizing this batch
	if !b.closing {
		l.batch = nil
		b.closing = true
		b.end(l)
	}

}

func (b *LoaderBatch[K, R]) end(l *Loader[K, R]) {
	b.one.Do(func() {
		b.data, b.error = l.fetch(b.keys)
		close(b.done)
	})
}
