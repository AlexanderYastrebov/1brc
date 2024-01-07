package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestRoundJava(t *testing.T) {
	for _, tc := range []struct {
		value    float64
		expected string
	}{
		{value: -1.5, expected: "-1.0"},
		{value: -1.0, expected: "-1.0"},
		{value: -0.7, expected: "-1.0"},
		{value: -0.5, expected: "0.0"},
		{value: -0.3, expected: "0.0"},
		{value: 0.0, expected: "0.0"},
		{value: 0.3, expected: "0.0"},
		{value: 0.5, expected: "1.0"},
		{value: 0.7, expected: "1.0"},
		{value: 1.0, expected: "1.0"},
		{value: 1.5, expected: "2.0"},
	} {
		if rounded := roundJava(tc.value); fmt.Sprintf("%.1f", rounded) != tc.expected {
			t.Errorf("Wrong rounding of %v, expected: %s, got: %.1f", tc.value, tc.expected, rounded)
		}
	}
}

func TestParseNumber(t *testing.T) {
	for _, tc := range []struct {
		value    string
		expected string
	}{
		{value: "-99.9", expected: "-999"},
		{value: "-12.3", expected: "-123"},
		{value: "-1.5", expected: "-15"},
		{value: "-1.0", expected: "-10"},
		{value: "0.0", expected: "0"},
		{value: "0.3", expected: "3"},
		{value: "12.3", expected: "123"},
		{value: "99.9", expected: "999"},
	} {
		if number := parseNumber([]byte(tc.value)); fmt.Sprintf("%d", number) != tc.expected {
			t.Errorf("Wrong parsing of %v, expected: %s, got: %d", tc.value, tc.expected, number)
		}
	}
}

var parseNumberSink int64

func BenchmarkParseNumber(b *testing.B) {
	data1 := []byte("1.2")
	data2 := []byte("-12.3")

	for i := 0; i < b.N; i++ {
		parseNumberSink = parseNumber(data1) + parseNumber(data2)
	}
}

func TestParseNumberLE(t *testing.T) {
	for _, tc := range []struct {
		value    string
		expected string
		size     int
	}{
		{value: "-99.9___", expected: "-999", size: 5},
		{value: "-12.3___", expected: "-123", size: 5},
		{value: "-1.5____", expected: "-15", size: 4},
		{value: "-1.0____", expected: "-10", size: 4},
		{value: "0.0_____", expected: "0", size: 3},
		{value: "0.3_____", expected: "3", size: 3},
		{value: "12.3____", expected: "123", size: 4},
		{value: "99.9____", expected: "999", size: 4},
	} {
		x := binary.LittleEndian.Uint64([]byte(tc.value))

		number, size := parseNumberLE(x)
		if fmt.Sprintf("%d", number) != tc.expected {
			t.Errorf("Wrong parsing of %v, expected: %s, got: %d", tc.value, tc.expected, number)
		}

		if size != tc.size {
			t.Errorf("Wrong parsing of %v, expected size: %d, got: %d", tc.value, tc.size, size)
		}
	}
}

func BenchmarkParseNumberLE(b *testing.B) {
	x1 := binary.LittleEndian.Uint64([]byte("1.2_____"))
	x2 := binary.LittleEndian.Uint64([]byte("-12.3___"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v1, _ := parseNumberLE(x1)
		v2, _ := parseNumberLE(x2)
		parseNumberSink = v1 + v2
	}
}

func BenchmarkProcess(b *testing.B) {
	// $ ./create_measurements.sh 1000000 && mv measurements.txt measurements-1e6.txt
	// Created file with 1,000,000 measurements in 514 ms
	const filename = "../../../measurements-1e6.txt"

	data, err := os.ReadFile(filename)
	if err != nil {
		b.Fatal(err)
	}

	measurements := process(data)
	rows := int64(0)
	for _, m := range measurements {
		rows += m.count
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(rows), "rows/op")

	for i := 0; i < b.N; i++ {
		process(data)
	}
}
