package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
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

	f, err := os.Open(os.Args[1])
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

	measurements := make(map[string]*measurement)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		id, val, _ := strings.Cut(line, ";")

		temp, _ := strconv.ParseFloat(val, 64)

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
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

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

func round(v float64) float64 {
	return math.Round(v*10.0) / 10.0
}
