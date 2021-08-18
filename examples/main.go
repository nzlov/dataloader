package main

import (
	"errors"
	"fmt"

	"github.com/nzlov/dataloader"
)

func main() {
	config := dataloader.NewConfig()
	strLoader := dataloader.NewLoader[string](config, func(keys []string) ([]*string, []error) {
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
	intLoader := dataloader.NewLoader[int](config, func(keys []string) ([]*int, []error) {
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
	i, errs := intLoader.LoadAll([]string{"a", "c"})
	fmt.Println(i, errs)
}
