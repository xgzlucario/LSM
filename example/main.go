package main

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
	lsm "github.com/xgzlucario/LSM"
	"github.com/xgzlucario/LSM/option"
)

func main() {
	lsm, err := lsm.NewLSM("data", option.DefaultOption)
	if err != nil {
		panic(err)
	}

	for {
		k := []byte(gofakeit.Phone())
		lsm.Put(k, k)

		time.Sleep(time.Microsecond / 10)
	}
}
