package main

import "testing"

func TestRoundJava(t *testing.T) {
	for _, tc := range []struct {
		value, expected float64
	}{
		{value: -1.5, expected: -1.0},
		{value: -1.0, expected: -1.0},
		{value: -0.7, expected: -1.0},
		{value: -0.5, expected: 0.0},
		{value: -0.3, expected: 0.0},
		{value: 0.0, expected: 0.0},
		{value: 0.3, expected: 0.0},
		{value: 0.5, expected: 1.0},
		{value: 0.7, expected: 1.0},
		{value: 1.0, expected: 1.0},
		{value: 1.5, expected: 2.0},
	} {
		if rounded := roundJava(tc.value); rounded != tc.expected {
			t.Errorf("Wrong rounding of %v, expected: %v, got: %v", tc.value, tc.expected, rounded)
		}
	}
}
