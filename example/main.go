package main

import (
	"strconv"
	"time"

	lsm "github.com/xgzlucario/LSM"
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

	lsm, err := lsm.NewLSM("lsm")
	if err != nil {
		panic(err)
	}

	for i := 0; ; i++ {
		k := []byte("key" + strconv.Itoa(i))
		lsm.Put(k, k)

		time.Sleep(time.Microsecond)
	}
}
