package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	lsm "github.com/xgzlucario/LSM"
)

func main() {
	cfg := lsm.DefaultConfig

	memtable := lsm.NewMemTable(cfg.MemTableSize)
	for i := 0; i < 100; i++ {
		k := []byte("key" + strconv.Itoa(i))
		memtable.Put(k, k)
	}

	src := lsm.DumpTable(memtable, cfg)
	os.WriteFile("test.sst", src, 0644)

	res, err := lsm.FindTable([]byte("key50"), "test.sst")
	fmt.Println(string(res), err)

	time.Sleep(time.Hour)

	// The returned DB instance is safe for concurrent use. Which mean that all
	// DB's methods may be called concurrently from multiple goroutine.
	db, err := leveldb.OpenFile("level", nil)
	if err != nil {
		panic(err)
	}
	// ...
	defer db.Close()
	// ...

	// for i := 0; i < 100*10000; i++ {
	// 	k := []byte("key" + strconv.Itoa(i))
	// 	db.Put(k, k, nil)
	// }

	// time.Sleep(time.Second)
}
