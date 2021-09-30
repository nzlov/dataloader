package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nzlov/dataloader"
)

type Cache struct {
	m map[string][]byte
}

func (c *Cache) SaveExpire(key string, t time.Duration, data []byte) {
	c.m[key] = data
}
func (c *Cache) GetExpire(key string, t time.Duration) ([]byte, bool) {
	data, ok := c.m[key]
	return data, ok
}
func (c *Cache) Clear(keys ...string) {
	for _, v := range keys {
		delete(c.m, v)
	}
}

type A struct {
	a int

	intLoader2 *dataloader.Loader[string, dataloader.Config]
	intLoader3 *dataloader.Loader[string, os.File]
	intLoader4 *dataloader.Loader[string, os.File]
	intLoader  *dataloader.Loader[string, int]
}

func main() {
	config := dataloader.Config{
		Wait:      time.Second,
		CacheTime: time.Minute,
		MaxBatch:  100,
		Prefix:    "a",
	}
	cache := &Cache{
		m: map[string][]byte{},
	}

	strLoader := dataloader.NewLoader[string](config.NewPrefix("string:"), cache, func(keys []string) ([]*string, []error) {
		vs := []*string{}
		for _, v := range keys {
			vs = append(vs, &v)
		}
		return vs, nil
	})

	s, err := strLoader.Load("a")
	fmt.Println(*s, err)

	m := map[string]int{
		"a": 1,
		"b": 2,
	}
	a := A{}
	a.intLoader = dataloader.NewLoader[string, int](config.WithPrefix("int:"), cache, func(keys []string) ([]*int, []error) {
		fmt.Println(keys)
		vs := []*int{}
		errs := []error{}
		for _, v := range keys {
			mv, ok := m[v]
			if ok {
				vs = append(vs, &mv)
				errs = append(errs, nil)
			} else {
				vs = append(vs, nil)
				errs = append(errs, errors.New("not found"))
			}
		}
		return vs, errs
	})
	i, errs := a.intLoader.LoadAll([]string{"a", "c"})
	fmt.Println(i, errs)
	i, errs = a.intLoader.LoadAll([]string{"a", "c"})
	fmt.Println(i, errs)
	fmt.Println(cache.m)
}
