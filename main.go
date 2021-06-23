package main

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/rxdevgod/zcache"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	m := zcache.NewWithConfig(map[string]interface{}{
		"dbpath": "db/abc",
	})
	a := map[string]interface{}{}

	startTimeSet := time.Now().UnixNano()
	for i := 0; i < 100000000000; i++ {
		if i%100000 == 0 {
			curTimeSet := time.Now().UnixNano()
			fmt.Printf("RPS: %v, %v, %v : %v : %v \n", curTimeSet, startTimeSet, (curTimeSet-startTimeSet+1)/1000000000, i, int64(int64(i)*1000000000/int64(curTimeSet-startTimeSet+1)))
		}

		m.Set(strconv.Itoa(i), "bar")
	}

	fmt.Printf("DONE %v \n", a)
	time.Sleep(16000 * time.Second)
}
