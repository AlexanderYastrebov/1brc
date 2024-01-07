package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/bits"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
)

// See comment in process function for explanation.
const chunkOverlap = 4

type measurement struct {
	min, max, sum, count int64
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Missing measurements filename")
	}

	measurements := processFile(os.Args[1])

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

func processFile(filename string) map[string]*measurement {
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

	return process(data)
}

func process(data []byte) map[string]*measurement {
	nChunks := runtime.NumCPU()

	chunkSize := len(data) / nChunks
	if chunkSize == 0 {
		log.Fatalf("chunk size is zero due to size=%d and nChunks=%d", len(data), nChunks)
	}

	// Split data into chunks and process last row separately.
	// Each chunk ends with chunkOverlap bytes of the next chunk data.
	// This allows use of binary.LittleEndian.Uint64() to read 8 bytes at once.
	// Minimal row is "a;1.2\n" so we need to read up to 4 bytes of the next chunk
	// therefore chunkOverlap is 4.

	lastRowOffset := bytes.LastIndexByte(data[:len(data)-1], '\n')
	if lastRowOffset == -1 {
		// single row
		return parseRow(data)
	}

	lastRowOffset++

	chunks := make([]int, 0, nChunks)
	offset := 0
	for offset < len(data) {
		offset += chunkSize
		if offset >= lastRowOffset {
			chunks = append(chunks, lastRowOffset)
			break
		}

		nlPos := bytes.IndexByte(data[offset:lastRowOffset], '\n')
		if nlPos == -1 {
			chunks = append(chunks, lastRowOffset)
			break
		} else {
			offset += nlPos + 1
			chunks = append(chunks, offset)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	results := make([]map[string]*measurement, len(chunks)+1)
	start := 0
	for i, chunk := range chunks {
		// Let each chunk overlap into the next one,
		// processChunk accounts for this
		chunkData := data[start : chunk+chunkOverlap]

		go func(data []byte, i int) {
			results[i] = processChunk(data)
			wg.Done()
		}(chunkData, i)

		start = chunk
	}
	results[len(results)-1] = parseRow(data[lastRowOffset:])

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

func processChunk(data []byte) map[string]*measurement {
	// use uint64 FNV-1a hash of id value as buckets key and keep mapping to the id value.
	// This assumes no collisions of id hashes.
	const (
		// use power of 2 for fast modulo calculation
		nBuckets = 1 << 12
		maxIds   = 10_000

		fnv1aOffset64 = 14695981039346656037
		fnv1aPrime64  = 1099511628211
	)

	type entry struct {
		key uint64
		mid int
	}
	buckets := make([][]entry, nBuckets)
	measurements := make([]measurement, 0, maxIds)
	ids := make(map[uint64][]byte)

	getMeasurement := func(key uint64) *measurement {
		i := key & uint64(nBuckets-1)
		for j := 0; j < len(buckets[i]); j++ {
			e := &buckets[i][j]
			if e.key == key {
				return &measurements[e.mid]
			}
		}
		return nil
	}

	putMeasurement := func(key uint64, m measurement) {
		i := key & uint64(nBuckets-1)
		buckets[i] = append(buckets[i], entry{key: key, mid: len(measurements)})
		measurements = append(measurements, m)
	}

	// assume valid input
	// data contains chunkOverlap bytes of the next chunk at the end
	for len(data) > chunkOverlap {

		idHash := uint64(fnv1aOffset64)
		semiPos := 0
		for i, b := range data {
			if b == ';' {
				semiPos = i
				break
			}

			// calculate FNV-1a hash
			idHash ^= uint64(b)
			idHash *= fnv1aPrime64
		}

		idData := data[:semiPos]
		data = data[semiPos+1:]

		var temp int64
		// inlined parseNumberLE(x uint64) (int64, int)
		{
			x := binary.LittleEndian.Uint64(data)
			negative := (^x & 0x10) >> 4
			absolute := (x >> (negative << 3))
			dotPos := bits.TrailingZeros64(^absolute & 0x10_10_00)
			normShift := (20 - dotPos) & 8
			normalized := (absolute << normShift) & 0x0f_00_0f_0f
			value := ((normalized * 0x640a0001) >> 24) & 0x3ff
			temp = int64((value ^ -negative) + negative)

			size := int(negative) + (4 - (normShift >> 3))
			data = data[size+1:]
		}

		m := getMeasurement(idHash)
		if m == nil {
			putMeasurement(idHash, measurement{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			})
			ids[idHash] = idData
		} else {
			m.min = min(m.min, temp)
			m.max = max(m.max, temp)
			m.sum += temp
			m.count++
		}
	}

	result := make(map[string]*measurement, len(measurements))
	for _, bucket := range buckets {
		for _, entry := range bucket {
			result[string(ids[entry.key])] = &measurements[entry.mid]
		}
	}
	return result
}

// parseRow reads single row from the data
func parseRow(data []byte) map[string]*measurement {
	semiPos := bytes.IndexByte(data, ';')
	if semiPos == -1 || data[len(data)-1] != '\n' {
		log.Fatalf("invalid data: %s", data)
	}

	id := string(data[:semiPos])
	temp := parseNumber(data[semiPos+1 : len(data)-1])

	return map[string]*measurement{
		id: {
			min:   temp,
			max:   temp,
			sum:   temp,
			count: 1,
		},
	}
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

// parseNumberLE reads decimal number stored as bytes in little-endian order
// that matches "^-?[0-9]{1,2}[.][0-9]" pattern,
// e.g.: -12.3, -3.4, 5.6, 78.9 and returns the value*10, i.e. -123, -34, 56, 789.
// Inspired by CalculateAverage_merykitty.java parseDataPoint.
func parseNumberLE(x uint64) (int64, int) {
	// -99.9___ 5f5f5f39 2e 39 39 2d
	// -12.3___ 5f5f5f33 2e 32 31 2d
	// -1.5____ 5f5f5f5f 35 2e 31 2d
	// -1.0____ 5f5f5f5f 30 2e 31 2d
	// 0.0_____ 5f5f5f5f 5f 30 2e 30
	// 0.3_____ 5f5f5f5f 5f 33 2e 30
	// 12.3____ 5f5f5f5f 33 2e 32 31
	// 99.9____ 5f5f5f5f 39 2e 39 39

	// digits are 0x3*, '-' is 2d,
	// so check fifth bit to get sign
	// * 0 for positive
	// * 1 for negative
	negative := (^x & 0x10) >> 4

	// trim '-'
	absolute := (x >> (negative << 3))

	// digits are 0x3*, '.' is 2e,
	// so check fifth bit of second and third byte
	// of absolute value to get dot position
	// * 20 for two digits
	// * 12 for one digit
	dotPos := bits.TrailingZeros64(^absolute & 0x10_10_00)

	// calculate shift to add leading zero.
	// & 8 eliminates runtime.panicshift check
	// * 0 for two digits
	// * 8 for one digit
	normShift := (20 - dotPos) & 8

	// add leading zero to single digit number
	// and convert from ascii to BCD
	// -99.9___ 00000000 09 00 09 09
	// -12.3___ 00000000 03 00 02 01
	// -1.5____ 00000000 05 00 01 00
	// -1.0____ 00000000 00 00 01 00
	// 0.0_____ 00000000 00 00 00 00
	// 0.3_____ 00000000 03 00 00 00
	// 12.3____ 00000000 03 00 02 01
	// 99.9____ 00000000 09 00 09 09
	normalized := (absolute << normShift) & 0x0f_00_0f_0f

	// normalized number xy.z is now 00000000 z 00 y x
	// i.e we have normalized = z*0x01_00_00_00 + y*0x01_00 + x
	// and we need x*100 + y*10 + z.
	// TODO: explain this
	value := ((normalized * 0x640a0001) >> 24) & 0x3ff

	// negative is 0 or 1 so signed is either
	// value ^ 0 + 0 == value or
	// value ^ -1 + 1 == -value
	signed := int64((value ^ -negative) + negative)

	size := int(negative) + (4 - (normShift >> 3))

	if false {
		b := [8]byte{}
		binary.LittleEndian.PutUint64(b[:], x)
		fmt.Printf("%s %016x %016x %d %d %016x %d %d %d\n", b, x, absolute, negative, dotPos, normalized, value, signed, size)
	}

	return signed, size
}
