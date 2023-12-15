package main

import (
	"fmt"
	"time"

	lsm "github.com/xgzlucario/LSM"
	"github.com/xgzlucario/LSM/option"
)

func main() {
	// The returned DB instance is safe for concurrent use. Which mean that all
	// DB's methods may be called concurrently from multiple goroutine.
	// db, err := leveldb.OpenFile("level", nil)
	// if err != nil {
	// 	panic(err)
	// }
	// defer db.Close()

	// for i := 0; ; i++ {
	// 	k := []byte("key" + strconv.Itoa(i))
	// 	db.Put(k, k, nil)
	// }

	lsm, err := lsm.NewLSM("data", option.DefaultOption)
	if err != nil {
		panic(err)
	}

	num := 100 * 10000
	for i := 0; i < num; i++ {
		if i%3 == 0 {
			k := []byte(fmt.Sprintf("%06d", i))
			if err := lsm.Put(k, k); err != nil {
				panic(err)
			}
		}
		time.Sleep(time.Microsecond / 10)
	}

	for i := 0; i < num; i++ {
		if i%3 == 1 {
			k := []byte(fmt.Sprintf("%06d", i))
			if err := lsm.Put(k, k); err != nil {
				panic(err)
			}
		}
		time.Sleep(time.Microsecond / 10)
	}

	for i := 0; i < num; i++ {
		if i%3 == 2 {
			k := []byte(fmt.Sprintf("%06d", i))
			if err := lsm.Put(k, k); err != nil {
				panic(err)
			}
		}
		time.Sleep(time.Microsecond / 10)
	}
}
