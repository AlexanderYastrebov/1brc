package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"syscall"
)

type measurement struct {
	min, max, sum float64
	count         int64
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
		fmt.Printf("%s=%.1f/%.1f/%.1f", id, round(m.min), round(m.sum/float64(m.count)), round(m.max))
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

	return processChunk(data)
}

func processChunk(data []byte) map[string]*measurement {
	measurements := make(map[string]*measurement)

	// assume valid input
	for {
		semiPos := bytes.IndexByte(data, ';')
		if semiPos == -1 {
			break
		}
		id := string(data[:semiPos])

		data = data[semiPos+1:]
		nlPos := bytes.IndexByte(data, '\n')

		var temp float64
		if nlPos == -1 {
			temp, _ = strconv.ParseFloat(string(data), 64)
		} else {
			temp, _ = strconv.ParseFloat(string(data[:nlPos]), 64)
			data = data[nlPos+1:]
		}

		m := measurements[id]
		if m == nil {
			measurements[id] = &measurement{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
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

	return measurements
}

func round(x float64) float64 {
	return roundJava(x*10.0) / 10.0
}

// roundJava returns the closest integer to the argument, with ties
// rounding to positive infinity, see java's Math.round
func roundJava(x float64) float64 {
	t := math.Trunc(x)
	if x < 0.0 && t-x == 0.5 {
		return t
	}
	if math.Abs(x-t) >= 0.5 {
		return t + math.Copysign(1, x)
	}
	return t
}
