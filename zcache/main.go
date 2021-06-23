package zcache

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// gShardCount ...
var gShardLogInterval = time.Duration(200) // Millisecond
var gShardCount = 500                      // Fix number for persistent
var gShardMaxItemInqueue = 1000000
var gShardPath = "./db"

// ConcurrentMap ...
type ConcurrentMap []*ConcurrentMapShared

// ConcurrentMapShared ...
type ConcurrentMapShared struct {
	file         *os.File
	items        map[string]interface{}
	buffer       bytes.Buffer
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

func parseKeyValueFromString(fullString string) (key string, value string, found bool) {
	firstSlash := strings.Index(fullString, " ")
	if firstSlash == -1 {
		found = false
		key = ""
		value = ""
	} else {
		found = true
		key = fullString[:firstSlash]
		value = fullString[firstSlash+1:]
	}
	return
}

// New  ...
func New() ConcurrentMap {
	return new(map[string]interface{}{})
}

// NewWithConfig ...
func NewWithConfig(config map[string]interface{}) ConcurrentMap {
	return new(config)
}

func new(config map[string]interface{}) ConcurrentMap {
	if val, ok := config["interval"]; ok {
		gShardLogInterval = val.(time.Duration)
	}

	if val, ok := config["dbpath"]; ok {
		gShardPath = val.(string) + "/"
	}

	os.MkdirAll(gShardPath, os.ModePerm)
	m := make(ConcurrentMap, gShardCount)
	for i := 0; i < gShardCount; i++ {
		m[i] = &ConcurrentMapShared{
			items: make(map[string]interface{}),
		}
	}

	// Load persistent data
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < gShardCount; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup, i int) {
				defer wg.Done()
				shard := m[i]
				shard.Lock()
				dat, _ := ioutil.ReadFile(gShardPath + strconv.Itoa(i))
				sdat := bytes.Split(dat, []byte{'\n'})
				for _, l := range sdat {
					key, value, found := parseKeyValueFromString(string(l))
					if found {
						shard.items[key] = value
					}
				}
				shard.Unlock()

			}(&wg, i)
		}
		wg.Wait()

		// Periodic save persistent data
		ticker := time.NewTicker(gShardLogInterval * time.Millisecond)
		go func() {
			for range ticker.C {
				for i := 0; i < gShardCount; i++ {
					shard := m[i]
					shard.file, _ = os.OpenFile(gShardPath+strconv.Itoa(i), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
					shard.Lock()
					shard.file.WriteString(shard.buffer.String())
					shard.buffer.Reset()
					shard.Unlock()
					shard.file.Close()
				}
			}
		}()
	}()

	return m
}

// GetShard ...
func (m ConcurrentMap) GetShard(key string) *ConcurrentMapShared {
	return m[uint(fnv32(key))%uint(gShardCount)]
}

// MSet ...
func (m ConcurrentMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Set ...
func (m ConcurrentMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.buffer.WriteString(key + " " + value.(string) + "\n")
	shard.Unlock()
}

// SetZ ...
func (m ConcurrentMap) SetZ(key string, value interface{}) {
	shard := m.GetShard(key)
	shard.items[key] = value
	shard.buffer.WriteString(key + " " + value.(string) + "\n")
}

// UpsertCb ...
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Upsert ...
func (m ConcurrentMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetIfAbsent ...
func (m ConcurrentMap) SetIfAbsent(key string, value interface{}) bool {
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get ...
func (m ConcurrentMap) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count ...
func (m ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < gShardCount; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has ...
func (m ConcurrentMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove ...
func (m ConcurrentMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb ...
type RemoveCb func(key string, v interface{}, exists bool) bool

// RemoveCb ...
func (m ConcurrentMap) RemoveCb(key string, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop ...
func (m ConcurrentMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty ...
func (m ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple ...
type Tuple struct {
	Key string
	Val interface{}
}

// Iter ...
func (m ConcurrentMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// IterBuffered ...
func (m ConcurrentMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// Clear removes all items from map.
func (m ConcurrentMap) Clear() {
	for item := range m.IterBuffered() {
		m.Remove(item.Key)
	}
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot(m ConcurrentMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, gShardCount)
	wg := sync.WaitGroup{}
	wg.Add(gShardCount)
	// Foreach shard.
	for index, shard := range m {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all items as map[string]interface{}
func (m ConcurrentMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// IterCb ...
type IterCb func(key string, v interface{})

// IterCb ...
func (m ConcurrentMap) IterCb(fn IterCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys ...
func (m ConcurrentMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(gShardCount)
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON ...
func (m ConcurrentMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
