package main

import (
	"fmt"
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
		{value: "-1.5", expected: "-1.5"},
		{value: "-1.0", expected: "-1.0"},
		{value: "0.0", expected: "0.0"},
		{value: "0.3", expected: "0.3"},
		{value: "12.3", expected: "12.3"},
	} {
		if number := parseNumber([]byte(tc.value)); fmt.Sprintf("%.1f", number) != tc.expected {
			t.Errorf("Wrong parsing of %v, expected: %s, got: %.1f", tc.value, tc.expected, number)
		}
	}
}

func BenchmarkProcess(b *testing.B) {
	// $ ./create_measurements.sh 1000000 && mv measurements.txt measurements-106.txt
	// Created file with 1,000,000 measurements in 514 ms
	const filename = "../measurements-106.txt"

	measurements := process(filename)
	rows := int64(0)
	for _, m := range measurements {
		rows += m.count
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(rows), "rows/op")

	for i := 0; i < b.N; i++ {
		process(filename)
	}
}
