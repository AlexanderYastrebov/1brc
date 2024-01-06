package main

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
)

type measurement struct {
	min, max, sum, count int64
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Missing measurements filename")
	}

	measurements := process(os.Args[1])

	ids := make([]string, 0, len(measurements))
	for id := range measurements {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Print("{")
	for i, id := range ids {
		if i > 0 {
			fmt.Print(", ")
		}
		m := measurements[id]
		fmt.Printf("%s=%.1f/%.1f/%.1f", id, round(float64(m.min)/10.0), round(float64(m.sum)/10.0/float64(m.count)), round(float64(m.max)/10.0))
	}
	fmt.Println("}")
}

func process(filename string) map[string]*measurement {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("Stat: %v", err)
	}

	size := fi.Size()
	if size <= 0 || size != int64(int(size)) {
		log.Fatalf("Invalid file size: %d", size)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	defer func() {
		if err := syscall.Munmap(data); err != nil {
			log.Fatalf("Munmap: %v", err)
		}
	}()

	nChunks := runtime.NumCPU()

	chunkSize := len(data) / nChunks
	if chunkSize == 0 {
		log.Fatalf("chunk size is zero due to size=%d and nChunks=%d", size, nChunks)
	}

	chunks := make([]int, 0, nChunks)
	offset := 0
	for {
		offset += chunkSize
		if offset >= len(data) {
			chunks = append(chunks, len(data))
			break
		}

		nlPos := bytes.IndexByte(data[offset:], '\n')
		if nlPos == -1 {
			chunks = append(chunks, len(data))
			break
		} else {
			offset += nlPos + 1
			chunks = append(chunks, offset)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	results := make([]map[string]*measurement, len(chunks))
	start := 0
	for i, chunk := range chunks {
		go func(data []byte, i int) {
			results[i] = processChunk(data)
			wg.Done()
		}(data[start:chunk], i)
		start = chunk
	}
	wg.Wait()

	measurements := make(map[string]*measurement)
	for _, r := range results {
		for id, rm := range r {
			m := measurements[id]
			if m == nil {
				measurements[id] = rm
			} else {
				m.min = min(m.min, rm.min)
				m.max = max(m.max, rm.max)
				m.sum += rm.sum
				m.count += rm.count
			}
		}
	}
	return measurements
}

var maphashSeed = maphash.MakeSeed()

func processChunk(data []byte) map[string]*measurement {
	// use hash of id as measurements key and keep mapping to the value
	measurements := make(map[uint64]*measurement)
	ids := make(map[uint64][]byte)

	// assume valid input
	for {
		semiPos := bytes.IndexByte(data, ';')
		if semiPos == -1 {
			break
		}

		_ = data[semiPos] // eliminate bound check

		idData := data[:semiPos]
		id := maphash.Bytes(maphashSeed, idData)

		data = data[semiPos+1:]

		nlPos := bytes.IndexByte(data, '\n')

		numData := data
		if nlPos != -1 {
			_ = data[nlPos] // eliminate bound check

			numData = data[:nlPos]
			data = data[nlPos+1:]
		}

		temp := parseNumber(numData)

		if m, ok := measurements[id]; !ok {
			measurements[id] = &measurement{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
			ids[id] = idData
		} else {
			m.min = min(m.min, temp)
			m.max = max(m.max, temp)
			m.sum += temp
			m.count++
		}

		if nlPos == -1 {
			break
		}
	}

	result := make(map[string]*measurement, len(measurements))
	for id, m := range measurements {
		result[string(ids[id])] = m
	}
	return result
}

func round(x float64) float64 {
	return roundJava(x*10.0) / 10.0
}

// roundJava returns the closest integer to the argument, with ties
// rounding to positive infinity, see java's Math.round
func roundJava(x float64) float64 {
	t := math.Trunc(x)
	if x < 0.0 && t-x == 0.5 {
		//return t
	} else if math.Abs(x-t) >= 0.5 {
		t += math.Copysign(1, x)
	}

	if t == 0 { // check -0
		return 0.0
	}
	return t
}

// parseNumber reads decimal number that matches "^-?[0-9]{1,2}[.][0-9]" pattern,
// e.g.: -12.3, -3.4, 5.6, 78.9 and return the value*10, i.e. -123, -34, 56, 789.
func parseNumber(data []byte) int64 {
	negative := data[0] == '-'
	if negative {
		data = data[1:]
	}

	var result int64
	switch len(data) {
	// 1.2
	case 3:
		result = int64(data[0])*10 + int64(data[2]) - '0'*(10+1)
	// 12.3
	case 4:
		result = int64(data[0])*100 + int64(data[1])*10 + int64(data[3]) - '0'*(100+10+1)
	}

	if negative {
		return -result
	}
	return result
}
