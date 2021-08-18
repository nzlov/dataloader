package main

import (
	"fmt"
	"time"

	"github.com/nzlov/dataloader"
)

func a() {
	c := dataloader.LoaderConfig[string]{
		Fetch: func(keys []string) ([]*string, []error) {
			a := []*string{}
			for i := range keys {
				a = append(a, &keys[i])
			}
			return a, nil
		},
		Wait:      time.Millisecond,
		CacheTime: time.Second,
		MaxBatch:  100,
	}
	loader := dataloader.NewLoader(c)
	r, err := loader.Load("abbb")
	fmt.Println(*r, err)
}

var m = map[string]int{
	"a": 1,
	"b": 2,
}

func b() {
	c := dataloader.LoaderConfig[int]{
		Fetch: func(keys []string) ([]*int, []error) {
			a := []*int{}
			for _, v := range keys {
				mv, ok := m[v]
				if ok {
					a = append(a, &mv)
				} else {
					a = append(a, nil)
				}
			}
			return a, nil
		},
		Wait:      time.Millisecond,
		CacheTime: time.Second,
		MaxBatch:  100,
	}
	loader := dataloader.NewLoader(c)
	r, err := loader.Load("a")
	fmt.Println(*r, err)
}

func main() {
	a()
	b()
}
