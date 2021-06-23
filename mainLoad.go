package main

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"zdb/zcache"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	startTimeSet := time.Now().UnixNano()
	m := zcache.New()

	curTimeSet := time.Now().UnixNano()
	deltaTime := (curTimeSet - startTimeSet) / 1000000000
	totalItem := m.Count()

	tempIndex := strconv.Itoa(totalItem - 1)
	val, _ := m.Get(tempIndex)
	fmt.Printf("ITEM "+tempIndex+" : %v \n", val)

	fmt.Printf("DONE %v IN %v SPEED %v \n", totalItem, deltaTime, int64(totalItem)/(deltaTime+1))
	time.Sleep(16000 * time.Second)
}
