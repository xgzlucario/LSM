package main

import (
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	// The returned DB instance is safe for concurrent use. Which mean that all
	// DB's methods may be called concurrently from multiple goroutine.
	db, err := leveldb.OpenFile("level", nil)
	if err != nil {
		panic(err)
	}
	// ...
	defer db.Close()
	// ...

	for i := 0; i < 100*10000; i++ {
		k := []byte("key" + strconv.Itoa(i))
		db.Put(k, k, nil)
	}

	time.Sleep(time.Second)
}
